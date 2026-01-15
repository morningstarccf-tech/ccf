"""
MySQL 实例管理的 Django Admin 配置

提供实例、数据库、监控指标的后台管理界面。
"""
from django.contrib import admin
from django import forms
from django.utils.html import format_html
from django.urls import reverse
from django.utils.safestring import mark_safe
from apps.instances.models import MySQLInstance, Database, MonitoringMetrics


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
                'data_dir', 'xtrabackup_bin'
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
    
    def has_add_permission(self, request):
        """禁止手动添加监控数据"""
        return False
    
    def has_change_permission(self, request, obj=None):
        """禁止修改监控数据"""
        return False
    
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
        if value >= 90:
            color = 'red'
        elif value >= 70:
            color = 'orange'
        else:
            color = 'green'
        
        return format_html(
            '<span style="color: {};">{:.1f}%</span>',
            color, value
        )


# 自定义 Admin 站点标题
admin.site.site_header = 'DB-Guardian 管理后台'
admin.site.site_title = 'DB-Guardian'
admin.site.index_title = 'MySQL 数据库备份恢复系统'
