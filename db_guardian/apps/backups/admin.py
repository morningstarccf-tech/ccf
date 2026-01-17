"""
备份管理的 Admin 后台配置

提供备份策略和备份记录的后台管理界面。
"""
import json
from django.contrib import admin, messages
from django.http import HttpResponseRedirect
from django import forms
from django.utils.html import format_html
from django.urls import reverse, path
from django.shortcuts import get_object_or_404
from pathlib import Path
from apps.backups.models import BackupStrategy, BackupRecord
from apps.backups.tasks import execute_backup_task
from apps.backups.services import RestoreExecutor


@admin.register(BackupStrategy)
class BackupStrategyAdmin(admin.ModelAdmin):
    """
    备份策略 Admin 配置
    """

    class BackupStrategyForm(forms.ModelForm):
        databases = forms.CharField(
            label='数据库列表',
            required=False,
            widget=forms.Textarea(attrs={'rows': 2}),
            help_text='支持 JSON 数组或逗号分隔，如 ["db1","db2"] 或 db1,db2'
        )

        class Meta:
            model = BackupStrategy
            fields = '__all__'

        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            if self.instance and self.instance.pk and self.instance.databases:
                if isinstance(self.instance.databases, list):
                    self.fields['databases'].initial = ','.join(self.instance.databases)
                else:
                    self.fields['databases'].initial = str(self.instance.databases)

        def clean_databases(self):
            raw = self.cleaned_data.get('databases')
            if raw is None or raw == '':
                return []
            if isinstance(raw, list):
                return raw

            text = str(raw).strip()
            if not text:
                return []

            try:
                value = json.loads(text)
                if isinstance(value, list):
                    return [str(item).strip() for item in value if str(item).strip()]
                if isinstance(value, str):
                    text = value
                else:
                    raise forms.ValidationError('数据库列表必须是 JSON 数组或逗号分隔字符串')
            except json.JSONDecodeError:
                pass

            parts = [part.strip() for part in text.replace('\n', ',').split(',')]
            return [part for part in parts if part]

        def clean(self):
            cleaned_data = super().clean()
            backup_type = cleaned_data.get('backup_type')
            databases = cleaned_data.get('databases')
            if backup_type in ['hot', 'cold', 'incremental'] and databases:
                self.add_error('databases', '热备/冷备/增量备份不支持指定数据库列表')
            return cleaned_data

    form = BackupStrategyForm
    
    list_display = [
        'id', 'name', 'instance', 'backup_type', 'cron_expression',
        'retention_days', 'is_enabled_badge', 'compress', 'created_at'
    ]
    
    list_filter = [
        'is_enabled', 'backup_type', 'compress', 'created_at'
    ]
    
    search_fields = [
        'name', 'instance__alias', 'cron_expression'
    ]

    actions = ['trigger_backup_action']
    change_form_template = 'admin/backups/backupstrategy/change_form.html'
    
    readonly_fields = [
        'created_by', 'created_at', 'updated_at'
    ]
    
    fieldsets = (
        ('基本信息', {
            'fields': ('name', 'instance', 'databases')
        }),
        ('备份配置', {
            'fields': ('cron_expression', 'backup_type', 'retention_days', 'compress')
        }),
        ('存储设置', {
            'fields': ('storage_path', 'is_enabled')
        }),
        ('元数据', {
            'fields': ('created_by', 'created_at', 'updated_at'),
            'classes': ('collapse',)
        }),
    )
    
    list_per_page = 20
    
    def is_enabled_badge(self, obj):
        """显示启用状态徽章"""
        if obj.is_enabled:
            return format_html(
                '<span style="color: green; font-weight: bold;">✓ 启用</span>'
            )
        return format_html(
            '<span style="color: red;">✗ 禁用</span>'
        )
    is_enabled_badge.short_description = '状态'
    
    def save_model(self, request, obj, form, change):
        """保存时设置创建者"""
        if not change:  # 新建时
            obj.created_by = request.user
        super().save_model(request, obj, form, change)

    def response_change(self, request, obj):
        """处理立即备份按钮"""
        if "_run_backup_now" in request.POST:
            try:
                task = execute_backup_task.delay(strategy_id=obj.id)
                messages.success(
                    request,
                    f'已创建备份任务，任务ID: {task.id}'
                )
            except Exception as exc:
                messages.error(request, f'立即备份失败: {exc}')
            return HttpResponseRedirect(request.path)
        return super().response_change(request, obj)

    @admin.action(description='立即执行备份')
    def trigger_backup_action(self, request, queryset):
        """批量触发备份任务"""
        created_count = 0
        for strategy in queryset:
            try:
                execute_backup_task.delay(strategy_id=strategy.id)
                created_count += 1
            except Exception as exc:
                messages.error(request, f'{strategy.name} 触发失败: {exc}')
        if created_count:
            messages.success(request, f'已创建 {created_count} 个备份任务')


@admin.register(BackupRecord)
class BackupRecordAdmin(admin.ModelAdmin):
    """
    备份记录 Admin 配置
    """
    
    list_display = [
        'id', 'instance', 'database_name', 'backup_type',
        'status_badge', 'file_size_mb', 'start_time', 'duration',
        'download_link', 'restore_link'
    ]
    
    list_filter = [
        'status', 'backup_type', 'start_time', 'created_at'
    ]
    
    search_fields = [
        'instance__alias', 'database_name', 'file_path'
    ]
    
    readonly_fields = [
        'instance', 'strategy', 'database_name', 'backup_type',
        'status', 'file_path', 'remote_path', 'object_storage_path',
        'file_size_mb', 'start_time', 'end_time',
        'error_message', 'created_by', 'created_at'
    ]
    
    fieldsets = (
        ('备份信息', {
            'fields': ('instance', 'strategy', 'database_name', 'backup_type')
        }),
        ('执行状态', {
            'fields': ('status', 'start_time', 'end_time', 'error_message')
        }),
        ('文件信息', {
            'fields': ('file_path', 'remote_path', 'object_storage_path', 'file_size_mb')
        }),
        ('元数据', {
            'fields': ('created_by', 'created_at'),
            'classes': ('collapse',)
        }),
    )
    
    list_per_page = 20
    
    # 禁用添加和修改
    def has_add_permission(self, request):
        return False
    
    def has_change_permission(self, request, obj=None):
        return False
    
    def status_badge(self, obj):
        """显示状态徽章"""
        status_colors = {
            'pending': 'gray',
            'running': 'blue',
            'success': 'green',
            'failed': 'red',
        }
        color = status_colors.get(obj.status, 'gray')
        return format_html(
            '<span style="color: {}; font-weight: bold;">{}</span>',
            color,
            obj.get_status_display()
        )
    status_badge.short_description = '状态'
    
    def duration(self, obj):
        """显示备份耗时"""
        seconds = obj.get_duration_seconds()
        if seconds is not None:
            if seconds < 60:
                return f"{seconds:.1f} 秒"
            elif seconds < 3600:
                return f"{seconds/60:.1f} 分钟"
            else:
                return f"{seconds/3600:.1f} 小时"
        return "-"
    duration.short_description = '耗时'

    def download_link(self, obj):
        """显示下载链接"""
        if obj.status != 'success' or not obj.file_path:
            return '-'
        file_path = Path(obj.file_path)
        if not file_path.exists():
            return '-'
        url = f"/api/backups/records/{obj.id}/download/"
        return format_html('<a href="{}">下载</a>', url)
    download_link.short_description = '下载'

    def restore_link(self, obj):
        """显示恢复链接（仅支持成功的全量备份）"""
        if obj.status != 'success' or obj.backup_type != 'full':
            return '-'
        if not obj.file_path or not Path(obj.file_path).exists():
            return '-'
        url = reverse('admin:backups_backuprecord_restore', args=[obj.id])
        return format_html(
            '<a href="{}" onclick="return confirm(\'确认要恢复该备份吗？\')">恢复</a>',
            url
        )
    restore_link.short_description = '恢复'

    def get_urls(self):
        """添加恢复操作的自定义路由"""
        urls = super().get_urls()
        custom_urls = [
            path(
                '<int:record_id>/restore/',
                self.admin_site.admin_view(self.restore_view),
                name='backups_backuprecord_restore'
            ),
        ]
        return custom_urls + urls

    def restore_view(self, request, record_id):
        """从备份记录恢复数据"""
        record = get_object_or_404(BackupRecord, pk=record_id)
        redirect_url = request.META.get(
            'HTTP_REFERER',
            reverse('admin:backups_backuprecord_changelist')
        )

        if record.status != 'success':
            messages.error(request, '只能从成功的备份中恢复')
            return HttpResponseRedirect(redirect_url)

        if record.backup_type in ['hot', 'cold', 'incremental']:
            messages.error(request, '热备/冷备/增量备份暂不支持在线恢复')
            return HttpResponseRedirect(redirect_url)

        if not record.file_path or not Path(record.file_path).exists():
            messages.error(request, '备份文件不存在，无法恢复')
            return HttpResponseRedirect(redirect_url)

        target_db = request.GET.get('target_db') or None
        executor = RestoreExecutor(record.instance)
        result = executor.execute_restore(record.file_path, target_db)
        if result.get('success'):
            messages.success(request, '恢复完成')
        else:
            messages.error(request, result.get('error_message', '恢复失败'))

        return HttpResponseRedirect(redirect_url)
