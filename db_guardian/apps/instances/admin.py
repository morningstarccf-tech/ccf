"""
MySQL 实例管理的 Django Admin 配置

提供实例、数据库、监控指标的后台管理界面。
"""
from django.contrib import admin, messages
from django import forms
from django.utils.html import format_html
from django.urls import reverse, path
from django.utils.safestring import mark_safe
from django.http import HttpResponseRedirect, JsonResponse
from django.db.models import OuterRef, Subquery
from django.template.response import TemplateResponse
from django.shortcuts import get_object_or_404
from apps.instances.models import MySQLInstance, Database, MonitoringMetrics
from apps.instances.services import DatabaseSyncService
from apps.backups.tasks import execute_backup_task


@admin.register(MySQLInstance)
class MySQLInstanceAdmin(admin.ModelAdmin):
    """MySQL 实例管理后台"""
    
    list_display = [
        'alias', 'host', 'port', 'team', 
        'status_badge', 'version', 'database_count',
        'last_check_time', 'created_at'
    ]
    list_filter = ['status', 'team', 'created_at']
    search_fields = ['alias', 'host', 'description']
    actions = ['trigger_backup_action']
    change_list_template = 'admin/instances/mysqlinstance/change_list.html'
    change_form_template = 'admin/instances/mysqlinstance/change_form.html'
    # 使用自定义表单，密码字段通过 PasswordInput 输入，不在表单中回显已加密内容
    class MySQLInstanceForm(forms.ModelForm):
        password = forms.CharField(
            label='密码',
            required=False,
            widget=forms.PasswordInput(render_value=False),
            help_text='MySQL 连接密码（留空则不修改，创建时必填）'
        )
        ssh_password = forms.CharField(
            label='SSH 密码',
            required=False,
            widget=forms.PasswordInput(render_value=False),
            help_text='SSH 密码（留空则不修改）'
        )

        class Meta:
            model = MySQLInstance
            fields = '__all__'

        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            # 不将数据库中加密的密码回显到表单中
            if self.instance and self.instance.pk:
                self.fields['password'].initial = ''
                self.fields['ssh_password'].initial = ''

        def clean(self):
            """保持密码为空时不覆盖已有值，创建时强制填写密码。"""
            cleaned_data = super().clean()

            if self.instance and self.instance.pk:
                if not cleaned_data.get('password'):
                    cleaned_data['password'] = self.instance.password
                if not cleaned_data.get('ssh_password'):
                    cleaned_data['ssh_password'] = self.instance.ssh_password
            else:
                if not cleaned_data.get('password'):
                    self.add_error('password', '创建时必须填写密码')

            return cleaned_data

    form = MySQLInstanceForm

    readonly_fields = [
        'status', 'last_check_time', 'version',
        'created_by', 'created_at', 'updated_at',
        'password_info', 'ssh_password_info'
    ]
    
    fieldsets = (
        ('基本信息', {
            'fields': ('alias', 'description', 'team', 'created_by')
        }),
        ('连接配置', {
            'fields': ('host', 'port', 'username', 'password', 'charset')
        }),
        ('部署与备份配置', {
            'fields': (
                'deployment_type', 'docker_container_name', 'mysql_service_name',
                'data_dir', 'remote_backup_root', 'xtrabackup_bin'
            )
        }),
        ('远程执行（SSH）', {
            'fields': ('ssh_host', 'ssh_port', 'ssh_user', 'ssh_password', 'ssh_key_path')
        }),
        ('状态信息', {
            'fields': ('status', 'version', 'last_check_time')
        }),
        ('时间信息', {
            'fields': ('created_at', 'updated_at'),
            'classes': ('collapse',)
        }),
    )
    
    def status_badge(self, obj):
        """状态徽章"""
        colors = {
            'online': 'green',
            'offline': 'orange',
            'error': 'red',
        }
        return format_html(
            '<span style="color: {};">● {}</span>',
            colors.get(obj.status, 'gray'),
            obj.get_status_display()
        )
    status_badge.short_description = '状态'
    
    def database_count(self, obj):
        """数据库数量"""
        count = obj.databases.count()
        url = reverse('admin:instances_database_changelist') + f'?instance__id__exact={obj.id}'
        return format_html('<a href="{}">{} 个</a>', url, count)
    database_count.short_description = '数据库数量'
    
    def password_info(self, obj):
        """密码信息（不显示明文）"""
        if obj.password:
            return format_html(
                '<span style="color: green;">已设置（加密存储）</span><br>'
                '<small style="color: gray;">密码使用 Fernet 加密算法安全存储</small>'
            )
        return format_html('<span style="color: red;">未设置</span>')
    password_info.short_description = '密码'

    def ssh_password_info(self, obj):
        """SSH 密码信息（不显示明文）"""
        if obj.ssh_password:
            return format_html(
                '<span style="color: green;">已设置（加密存储）</span><br>'
                '<small style="color: gray;">SSH 密码已加密存储</small>'
            )
        return format_html('<span style="color: red;">未设置</span>')
    ssh_password_info.short_description = 'SSH 密码'
    
    def save_model(self, request, obj, form, change):
        """保存时设置创建者"""
        if not change:
            obj.created_by = request.user
        super().save_model(request, obj, form, change)

    def response_change(self, request, obj):
        """处理同步按钮的自定义操作"""
        if "_sync_databases" in request.POST:
            try:
                result = DatabaseSyncService.sync_databases(
                    obj,
                    refresh_stats=True,
                    include_system=True
                )
                messages.success(
                    request,
                    f'同步完成，新增 {result["created"]} 个，更新 {result["updated"]} 个，删除 {result.get("deleted", 0)} 个'
                )
            except Exception as exc:
                messages.error(request, f'{obj.alias} 同步失败: {exc}')
            return HttpResponseRedirect(request.path)
        if "_run_backup_now" in request.POST:
            try:
                task = execute_backup_task.delay(
                    instance_id=obj.id,
                    user_id=request.user.id,
                    backup_type='full',
                    compress=True
                )
                messages.success(
                    request,
                    f'已创建备份任务，任务ID: {task.id}'
                )
            except Exception as exc:
                messages.error(request, f'立即备份失败: {exc}')
            return HttpResponseRedirect(request.path)
        return super().response_change(request, obj)

    def get_urls(self):
        urls = super().get_urls()
        custom_urls = [
            path(
                'refresh-all/',
                self.admin_site.admin_view(self.refresh_all_view),
                name='instances_mysqlinstance_refresh_all'
            )
        ]
        return custom_urls + urls

    def refresh_all_view(self, request):
        if request.method != 'POST':
            return HttpResponseRedirect(reverse('admin:instances_mysqlinstance_changelist'))
        instances = MySQLInstance.objects.all()
        created_total = 0
        updated_total = 0
        deleted_total = 0
        for instance in instances:
            try:
                result = DatabaseSyncService.sync_databases(
                    instance,
                    refresh_stats=True,
                    include_system=True
                )
                created_total += result['created']
                updated_total += result['updated']
                deleted_total += result.get('deleted', 0)
            except Exception as exc:
                messages.error(request, f'{instance.alias} 同步失败: {exc}')
        messages.success(
            request,
            f'已刷新所有实例：新增 {created_total} 个，更新 {updated_total} 个，删除 {deleted_total} 个'
        )
        return HttpResponseRedirect(reverse('admin:instances_mysqlinstance_changelist'))

    @admin.action(description='立即执行备份')
    def trigger_backup_action(self, request, queryset):
        """批量触发备份任务"""
        created_count = 0
        for instance in queryset:
            try:
                execute_backup_task.delay(
                    instance_id=instance.id,
                    user_id=request.user.id,
                    backup_type='full',
                    compress=True
                )
                created_count += 1
            except Exception as exc:
                messages.error(request, f'{instance.alias} 触发失败: {exc}')
        if created_count:
            messages.success(request, f'已创建 {created_count} 个备份任务')


class DatabaseInline(admin.TabularInline):
    """数据库内联显示"""
    model = Database
    extra = 0
    readonly_fields = ['size_mb', 'table_count', 'last_backup_time', 'created_at']
    fields = ['name', 'charset', 'collation', 'size_mb', 'table_count', 'last_backup_time']


@admin.register(Database)
class DatabaseAdmin(admin.ModelAdmin):
    """数据库管理后台"""
    
    list_display = [
        'name', 'instance', 'charset', 'collation',
        'size_display', 'table_count', 'last_backup_time'
    ]
    list_filter = ['instance', 'charset', 'created_at']
    search_fields = ['name', 'instance__alias']
    readonly_fields = ['size_mb', 'table_count', 'last_backup_time', 'created_at', 'updated_at']
    actions = None
    change_list_template = 'admin/instances/database/instance_list.html'
    
    def has_add_permission(self, request):
        return False
    
    fieldsets = (
        ('基本信息', {
            'fields': ('instance', 'name')
        }),
        ('字符集配置', {
            'fields': ('charset', 'collation')
        }),
        ('统计信息', {
            'fields': ('size_mb', 'table_count', 'last_backup_time')
        }),
        ('时间信息', {
            'fields': ('created_at', 'updated_at'),
            'classes': ('collapse',)
        }),
    )
    
    def size_display(self, obj):
        """格式化显示大小"""
        if obj.size_mb < 1024:
            return f"{obj.size_mb:.2f} MB"
        else:
            return f"{obj.size_mb / 1024:.2f} GB"
    size_display.short_description = '大小'
    size_display.admin_order_field = 'size_mb'

    def changelist_view(self, request, extra_context=None):
        instances = MySQLInstance.objects.prefetch_related('databases').order_by('alias')
        context = {
            **self.admin_site.each_context(request),
            'title': '数据库',
            'instances': instances,
            'refresh_url': reverse('admin:instances_database_refresh_all')
        }
        if extra_context:
            context.update(extra_context)
        return TemplateResponse(request, self.change_list_template, context)

    @admin.action(description='同步所属实例数据库并刷新统计')
    def sync_related_instances_action(self, request, queryset):
        """批量同步所选数据库所属的实例"""
        instances = {db.instance for db in queryset.select_related('instance')}
        created_total = 0
        updated_total = 0
        deleted_total = 0
        for instance in instances:
            try:
                result = DatabaseSyncService.sync_databases(
                    instance,
                    refresh_stats=True,
                    include_system=True
                )
                created_total += result['created']
                updated_total += result['updated']
                deleted_total += result.get('deleted', 0)
            except Exception as exc:
                messages.error(request, f'{instance.alias} 同步失败: {exc}')
        messages.success(
            request,
            f'同步完成，新增 {created_total} 个，更新 {updated_total} 个，删除 {deleted_total} 个'
        )

    def get_urls(self):
        urls = super().get_urls()
        custom_urls = [
            path(
                'refresh-all/',
                self.admin_site.admin_view(self.refresh_all_view),
                name='instances_database_refresh_all'
            )
        ]
        return custom_urls + urls

    def refresh_all_view(self, request):
        if request.method != 'POST':
            return HttpResponseRedirect(reverse('admin:instances_database_changelist'))
        instances = MySQLInstance.objects.all()
        created_total = 0
        updated_total = 0
        deleted_total = 0
        for instance in instances:
            try:
                result = DatabaseSyncService.sync_databases(
                    instance,
                    refresh_stats=True,
                    include_system=True
                )
                created_total += result['created']
                updated_total += result['updated']
                deleted_total += result.get('deleted', 0)
            except Exception as exc:
                messages.error(request, f'{instance.alias} 同步失败: {exc}')
        messages.success(
            request,
            f'已刷新所有实例：新增 {created_total} 个，更新 {updated_total} 个，删除 {deleted_total} 个'
        )
        return HttpResponseRedirect(reverse('admin:instances_database_changelist'))


@admin.register(MonitoringMetrics)
class MonitoringMetricsAdmin(admin.ModelAdmin):
    """监控指标管理后台"""
    
    list_display = [
        'instance', 'timestamp', 
        'qps', 'tps', 'connections',
        'slow_queries', 'cpu_usage_display',
        'memory_usage_display', 'disk_usage_display'
    ]
    list_filter = ['instance', 'timestamp']
    search_fields = ['instance__alias']
    readonly_fields = [
        'instance', 'timestamp', 'qps', 'tps', 
        'connections', 'slow_queries',
        'cpu_usage', 'memory_usage', 'disk_usage'
    ]
    date_hierarchy = 'timestamp'
    change_list_template = 'admin/instances/monitoringmetrics/change_list.html'
    
    def has_add_permission(self, request):
        """禁止手动添加监控数据"""
        return False
    
    def has_change_permission(self, request, obj=None):
        """禁止修改监控数据"""
        return False

    def has_delete_permission(self, request, obj=None):
        return False

    def get_urls(self):
        urls = super().get_urls()
        custom_urls = [
            path(
                'realtime/',
                self.admin_site.admin_view(self.realtime_view),
                name='instances_monitoringmetrics_realtime'
            ),
        ]
        return custom_urls + urls

    def changelist_view(self, request, extra_context=None):
        latest = MonitoringMetrics.objects.filter(
            instance=OuterRef('pk')
        ).order_by('-timestamp')
        instances = MySQLInstance.objects.annotate(
            last_timestamp=Subquery(latest.values('timestamp')[:1]),
            last_cpu=Subquery(latest.values('cpu_usage')[:1]),
            last_memory=Subquery(latest.values('memory_usage')[:1]),
            last_disk=Subquery(latest.values('disk_usage')[:1])
        ).order_by('alias')
        context = {
            **self.admin_site.each_context(request),
            'title': '监控指标',
            'instances': instances,
            'realtime_url': reverse('admin:instances_monitoringmetrics_realtime')
        }
        if extra_context:
            context.update(extra_context)
        return TemplateResponse(request, self.change_list_template, context)

    def realtime_view(self, request):
        latest = MonitoringMetrics.objects.filter(
            instance=OuterRef('pk')
        ).order_by('-timestamp')
        instances = MySQLInstance.objects.annotate(
            last_timestamp=Subquery(latest.values('timestamp')[:1]),
            last_cpu=Subquery(latest.values('cpu_usage')[:1]),
            last_memory=Subquery(latest.values('memory_usage')[:1]),
            last_disk=Subquery(latest.values('disk_usage')[:1])
        ).order_by('alias')
        data = []
        for inst in instances:
            data.append({
                'id': inst.id,
                'alias': inst.alias,
                'timestamp': inst.last_timestamp.isoformat() if inst.last_timestamp else '',
                'cpu': float(inst.last_cpu or 0),
                'memory': float(inst.last_memory or 0),
                'disk': float(inst.last_disk or 0),
            })
        return JsonResponse({'success': True, 'data': data})
    
    def cpu_usage_display(self, obj):
        """CPU 使用率显示"""
        return self._usage_badge(obj.cpu_usage)
    cpu_usage_display.short_description = 'CPU'
    
    def memory_usage_display(self, obj):
        """内存使用率显示"""
        return self._usage_badge(obj.memory_usage)
    memory_usage_display.short_description = '内存'
    
    def disk_usage_display(self, obj):
        """磁盘使用率显示"""
        return self._usage_badge(obj.disk_usage)
    disk_usage_display.short_description = '磁盘'
    
    def _usage_badge(self, value):
        """使用率徽章"""
        try:
            numeric = float(value) if value is not None else 0.0
        except (TypeError, ValueError):
            numeric = 0.0

        if numeric >= 90:
            color = 'red'
        elif numeric >= 70:
            color = 'orange'
        else:
            color = 'green'
        
        value_display = f"{numeric:.1f}%"
        return format_html(
            '<span style="color: {};">{}</span>',
            color, value_display
        )


# 自定义 Admin 站点标题
admin.site.site_header = 'AuroraVault 管理后台'
admin.site.site_title = 'AuroraVault'
admin.site.index_title = 'MySQL 数据库备份恢复系统'
