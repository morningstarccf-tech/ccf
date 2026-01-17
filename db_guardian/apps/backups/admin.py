"""
备份管理的 Admin 后台配置

提供备份策略和备份记录的后台管理界面。
"""
import json
import datetime
import logging
from django.contrib import admin, messages
from django.http import HttpResponseRedirect, FileResponse
from django.conf import settings
from django.template.response import TemplateResponse
from django import forms
from django.utils.html import format_html
from django.utils import timezone
from django.urls import reverse, path
from django.shortcuts import get_object_or_404
from pathlib import Path
from apps.backups.models import (
    BackupStrategy,
    BackupRecord,
    BackupOneOffTask,
    BackupTaskBoard,
    BackupRestoreBoard
)
from apps.backups.tasks import execute_backup_task, execute_oneoff_backup_task
from apps.backups.services import StrategyManager, RemoteExecutor, ObjectStorageUploader
from apps.backups.services import RestoreExecutor

logger = logging.getLogger(__name__)
try:
    from django_celery_beat.models import (
        PeriodicTask,
        CrontabSchedule,
        IntervalSchedule,
        SolarSchedule,
        ClockedSchedule
    )
except Exception:
    PeriodicTask = None
    CrontabSchedule = None
    IntervalSchedule = None
    SolarSchedule = None
    ClockedSchedule = None

@admin.register(BackupStrategy)
class BackupStrategyAdmin(admin.ModelAdmin):
    """
    备份策略 Admin 配置
    """

    class BackupStrategyForm(forms.ModelForm):
        cron_expression = forms.CharField(
            required=False,
            widget=forms.HiddenInput()
        )
        storage_target = forms.ChoiceField(
            label='存储位置',
            choices=[
                ('local', '本地路径'),
                ('remote', '远程服务器路径'),
                ('oss', '云存储（OSS）'),
            ],
            initial='local'
        )
        store_local = forms.BooleanField(
            label='本地保存',
            required=False,
            initial=True
        )
        store_remote = forms.BooleanField(
            label='远程保存',
            required=False
        )
        store_oss = forms.BooleanField(
            label='云存储保存',
            required=False
        )
        databases = forms.CharField(
            label='数据库列表',
            required=False,
            widget=forms.Textarea(attrs={'rows': 2}),
            help_text='支持 JSON 数组或逗号分隔，如 ["db1","db2"] 或 db1,db2'
        )
        schedule_type = forms.ChoiceField(
            label='周期类型',
            choices=[
                ('daily', '每天'),
                ('weekly', '每周'),
                ('monthly', '每月'),
                ('hourly', '每小时'),
            ],
            initial='daily'
        )
        schedule_time = forms.TimeField(
            label='执行时间',
            required=False,
            input_formats=['%H:%M'],
            widget=forms.TimeInput(format='%H:%M'),
            help_text='24 小时制，如 08:30'
        )
        schedule_weekday = forms.ChoiceField(
            label='星期',
            required=False,
            choices=[
                ('1', '周一'),
                ('2', '周二'),
                ('3', '周三'),
                ('4', '周四'),
                ('5', '周五'),
                ('6', '周六'),
                ('0', '周日'),
            ]
        )
        schedule_day = forms.IntegerField(
            label='每月日期',
            required=False,
            min_value=1,
            max_value=31
        )
        schedule_minute = forms.IntegerField(
            label='每小时分钟',
            required=False,
            min_value=0,
            max_value=59
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
            self.fields['store_local'].widget = forms.HiddenInput()
            self.fields['store_remote'].widget = forms.HiddenInput()
            self.fields['store_oss'].widget = forms.HiddenInput()
            self._apply_storage_target_initial()
            self._apply_schedule_initial()

        def _apply_storage_target_initial(self):
            if not self.instance:
                return
            if self.instance.store_remote:
                self.initial['storage_target'] = 'remote'
            elif self.instance.store_oss:
                self.initial['storage_target'] = 'oss'
            else:
                self.initial['storage_target'] = 'local'

        def _apply_schedule_initial(self):
            cron_expr = self.instance.cron_expression if self.instance else None
            if not cron_expr:
                return
            parts = cron_expr.strip().split()
            if len(parts) != 5:
                return

            minute, hour, day_of_month, _month_of_year, day_of_week = parts
            if day_of_month == '*' and day_of_week == '*':
                if hour == '*':
                    self.initial['schedule_type'] = 'hourly'
                    if minute.isdigit():
                        self.initial['schedule_minute'] = int(minute)
                else:
                    self.initial['schedule_type'] = 'daily'
                    if hour.isdigit() and minute.isdigit():
                        self.initial['schedule_time'] = datetime.time(int(hour), int(minute))
                return

            if day_of_month == '*' and day_of_week != '*':
                self.initial['schedule_type'] = 'weekly'
                self.initial['schedule_weekday'] = day_of_week
                if hour.isdigit() and minute.isdigit():
                    self.initial['schedule_time'] = datetime.time(int(hour), int(minute))
                return

            if day_of_month != '*' and day_of_week == '*':
                self.initial['schedule_type'] = 'monthly'
                if day_of_month.isdigit():
                    self.initial['schedule_day'] = int(day_of_month)
                if hour.isdigit() and minute.isdigit():
                    self.initial['schedule_time'] = datetime.time(int(hour), int(minute))

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
            schedule_type = cleaned_data.get('schedule_type')
            schedule_time = cleaned_data.get('schedule_time')
            schedule_weekday = cleaned_data.get('schedule_weekday')
            schedule_day = cleaned_data.get('schedule_day')
            schedule_minute = cleaned_data.get('schedule_minute')
            storage_target = cleaned_data.get('storage_target')

            if schedule_type == 'hourly':
                if schedule_minute is None:
                    self.add_error('schedule_minute', '请选择每小时分钟')
                else:
                    cleaned_data['cron_expression'] = f"{schedule_minute} * * * *"
            elif schedule_type == 'daily':
                if not schedule_time:
                    self.add_error('schedule_time', '请选择执行时间')
                else:
                    cleaned_data['cron_expression'] = f"{schedule_time.minute} {schedule_time.hour} * * *"
            elif schedule_type == 'weekly':
                if not schedule_time:
                    self.add_error('schedule_time', '请选择执行时间')
                if not schedule_weekday:
                    self.add_error('schedule_weekday', '请选择星期')
                if schedule_time and schedule_weekday:
                    cleaned_data['cron_expression'] = f"{schedule_time.minute} {schedule_time.hour} * * {schedule_weekday}"
            elif schedule_type == 'monthly':
                if not schedule_time:
                    self.add_error('schedule_time', '请选择执行时间')
                if not schedule_day:
                    self.add_error('schedule_day', '请选择每月日期')
                if schedule_time and schedule_day:
                    cleaned_data['cron_expression'] = f"{schedule_time.minute} {schedule_time.hour} {schedule_day} * *"

            backup_type = cleaned_data.get('backup_type')
            databases = cleaned_data.get('databases')
            remote_storage_path = cleaned_data.get('remote_storage_path')
            instance = cleaned_data.get('instance')

            store_local = storage_target == 'local'
            store_remote = storage_target == 'remote'
            store_oss = storage_target == 'oss'
            cleaned_data['store_local'] = store_local
            cleaned_data['store_remote'] = store_remote
            cleaned_data['store_oss'] = store_oss

            if store_remote and not remote_storage_path:
                if not (instance and instance.remote_backup_root):
                    self.add_error('remote_storage_path', '请填写远程存储路径或在实例中配置远程备份目录')

            if backup_type in ['hot', 'cold', 'incremental'] and databases:
                self.add_error('databases', '热备/冷备/增量备份不支持指定数据库列表')
            return cleaned_data

    form = BackupStrategyForm
    
    list_display = [
        'id', 'name', 'instance', 'backup_type', 'schedule_display',
        'retention_days', 'is_enabled_badge', 'compress', 'created_at'
    ]
    
    list_filter = [
        'is_enabled', 'backup_type', 'compress', 'created_at'
    ]
    
    search_fields = [
        'name', 'instance__alias', 'cron_expression'
    ]

    actions = ['trigger_backup_action', 'enable_strategy_action', 'disable_strategy_action']
    change_form_template = 'admin/backups/backupstrategy/change_form.html'
    
    readonly_fields = [
        'created_by', 'created_at', 'updated_at'
    ]
    
    fieldsets = (
        ('基本信息', {
            'fields': ('name', 'instance', 'databases')
        }),
        ('备份配置', {
            'fields': (
                'schedule_type',
                'schedule_time',
                'schedule_weekday',
                'schedule_day',
                'schedule_minute',
                'cron_expression',
                'backup_type',
                'retention_days',
                'compress'
            )
        }),
        ('存储设置', {
            'fields': (
                'storage_path',
                'storage_target',
                'remote_storage_path',
                'is_enabled'
            )
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

    def schedule_display(self, obj):
        """显示调度规则"""
        return obj.get_schedule_display()
    schedule_display.short_description = '计划'
    
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
        if "_enable_strategy" in request.POST:
            if obj.is_enabled:
                messages.info(request, '策略已处于启用状态')
                return HttpResponseRedirect(request.path)
            try:
                obj.is_enabled = True
                obj.save(update_fields=['is_enabled'])
                StrategyManager.sync_to_celery_beat()
                messages.success(request, '策略已启用并同步到调度器')
            except Exception as exc:
                messages.error(request, f'启用策略失败: {exc}')
            return HttpResponseRedirect(request.path)
        if "_disable_strategy" in request.POST:
            if not obj.is_enabled:
                messages.info(request, '策略已处于禁用状态')
                return HttpResponseRedirect(request.path)
            try:
                obj.is_enabled = False
                obj.save(update_fields=['is_enabled'])
                StrategyManager.sync_to_celery_beat()
                messages.success(request, '策略已禁用并同步到调度器')
            except Exception as exc:
                messages.error(request, f'禁用策略失败: {exc}')
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

    @admin.action(description='启用备份策略')
    def enable_strategy_action(self, request, queryset):
        """批量启用策略"""
        updated = queryset.filter(is_enabled=False).update(is_enabled=True)
        if updated:
            StrategyManager.sync_to_celery_beat()
            messages.success(request, f'已启用 {updated} 个策略')
        else:
            messages.info(request, '没有需要启用的策略')

    @admin.action(description='禁用备份策略')
    def disable_strategy_action(self, request, queryset):
        """批量禁用策略"""
        updated = queryset.filter(is_enabled=True).update(is_enabled=False)
        if updated:
            StrategyManager.sync_to_celery_beat()
            messages.success(request, f'已禁用 {updated} 个策略')
        else:
            messages.info(request, '没有需要禁用的策略')


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
        if obj.status != 'success':
            return '-'
        if not (obj.file_path or obj.remote_path or obj.object_storage_path):
            return '-'
        url = reverse('admin:backups_backuprecord_download', args=[obj.id])
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
        """添加下载/恢复操作的自定义路由"""
        urls = super().get_urls()
        custom_urls = [
            path(
                '<int:record_id>/download/',
                self.admin_site.admin_view(self.download_view),
                name='backups_backuprecord_download'
            ),
            path(
                '<int:record_id>/restore/',
                self.admin_site.admin_view(self.restore_view),
                name='backups_backuprecord_restore'
            ),
        ]
        return custom_urls + urls

    def _infer_backup_filename(self, record):
        for path_value in [record.file_path, record.remote_path, record.object_storage_path]:
            if not path_value:
                continue
            if path_value.startswith('oss://'):
                stripped = path_value[len('oss://'):]
                _, _, key = stripped.partition('/')
                if key:
                    return Path(key).name
            return Path(path_value).name
        return f"backup_{record.id}.sql"

    def _prepare_download_path(self, record):
        if record.file_path:
            file_path = Path(record.file_path)
            if file_path.exists():
                return file_path

        backup_root = Path(getattr(settings, 'BACKUP_STORAGE_PATH', settings.BASE_DIR / 'backups'))
        temp_dir = backup_root / 'tmp'
        temp_dir.mkdir(parents=True, exist_ok=True)
        filename = self._infer_backup_filename(record)
        temp_path = temp_dir / filename

        if record.remote_path:
            executor = RemoteExecutor(record.instance)
            try:
                executor.download(record.remote_path, temp_path)
                if temp_path.exists():
                    return temp_path
            except Exception as exc:
                logger.warning(f"远程备份下载失败: {exc}")

        if record.object_storage_path:
            uploader = ObjectStorageUploader()
            try:
                uploader.download(record.object_storage_path, temp_path)
                if temp_path.exists():
                    return temp_path
            except Exception as exc:
                logger.warning(f"OSS 备份下载失败: {exc}")

        return None

    def download_view(self, request, record_id):
        record = get_object_or_404(BackupRecord, pk=record_id)
        redirect_url = request.META.get(
            'HTTP_REFERER',
            reverse('admin:backups_backuprecord_changelist')
        )
        if record.status != 'success':
            messages.error(request, '只能下载成功的备份文件')
            return HttpResponseRedirect(redirect_url)

        download_path = self._prepare_download_path(record)
        if not download_path:
            messages.error(request, '备份文件不存在或无法下载')
            return HttpResponseRedirect(redirect_url)

        return FileResponse(
            open(download_path, 'rb'),
            as_attachment=True,
            filename=download_path.name
        )

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


@admin.register(BackupOneOffTask)
class BackupOneOffTaskAdmin(admin.ModelAdmin):
    """
    定时任务 Admin 配置（一次性执行）
    """

    class BackupOneOffTaskForm(forms.ModelForm):
        databases = forms.CharField(
            label='数据库列表',
            required=False,
            widget=forms.Textarea(attrs={'rows': 2}),
            help_text='支持 JSON 数组或逗号分隔，如 ["db1","db2"] 或 db1,db2'
        )
        storage_target = forms.ChoiceField(
            label='存储位置',
            choices=[
                ('local', '本地路径'),
                ('remote', '远程服务器路径'),
                ('oss', '云存储（OSS）'),
            ],
            initial='local'
        )
        store_local = forms.BooleanField(
            label='本地保存',
            required=False,
            initial=True
        )
        store_remote = forms.BooleanField(
            label='远程保存',
            required=False
        )
        store_oss = forms.BooleanField(
            label='云存储保存',
            required=False
        )

        class Meta:
            model = BackupOneOffTask
            fields = '__all__'

        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            if self.instance and self.instance.pk and self.instance.databases:
                if isinstance(self.instance.databases, list):
                    self.fields['databases'].initial = ','.join(self.instance.databases)
                else:
                    self.fields['databases'].initial = str(self.instance.databases)
            self.fields['store_local'].widget = forms.HiddenInput()
            self.fields['store_remote'].widget = forms.HiddenInput()
            self.fields['store_oss'].widget = forms.HiddenInput()
            self._apply_storage_target_initial()

        def _apply_storage_target_initial(self):
            if not self.instance:
                return
            if self.instance.store_remote:
                self.initial['storage_target'] = 'remote'
            elif self.instance.store_oss:
                self.initial['storage_target'] = 'oss'
            else:
                self.initial['storage_target'] = 'local'

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
            storage_target = cleaned_data.get('storage_target')
            remote_storage_path = cleaned_data.get('remote_storage_path')
            instance = cleaned_data.get('instance')

            store_local = storage_target == 'local'
            store_remote = storage_target == 'remote'
            store_oss = storage_target == 'oss'
            cleaned_data['store_local'] = store_local
            cleaned_data['store_remote'] = store_remote
            cleaned_data['store_oss'] = store_oss

            if store_remote and not remote_storage_path:
                if not (instance and instance.remote_backup_root):
                    self.add_error('remote_storage_path', '请填写远程存储路径或在实例中配置远程备份目录')

            if backup_type in ['hot', 'cold', 'incremental'] and databases:
                self.add_error('databases', '热备/冷备/增量备份不支持指定数据库列表')
            return cleaned_data

    form = BackupOneOffTaskForm
    change_form_template = 'admin/backups/backuponeofftask/change_form.html'

    list_display = [
        'id', 'name', 'instance', 'backup_type', 'run_at',
        'status_badge', 'created_at', 'started_at', 'finished_at'
    ]

    list_filter = ['status', 'backup_type', 'run_at', 'created_at']
    search_fields = ['name', 'instance__alias']

    readonly_fields = [
        'task_id', 'backup_record', 'status', 'error_message',
        'created_by', 'created_at', 'started_at', 'finished_at'
    ]

    fieldsets = (
        ('任务信息', {
            'fields': (
                'name',
                'instance',
                'databases',
                'backup_type',
                'compress',
                'storage_path',
                'storage_target',
                'remote_storage_path',
                'run_at'
            )
        }),
        ('执行状态', {
            'fields': ('status', 'task_id', 'backup_record', 'error_message', 'started_at', 'finished_at')
        }),
        ('元数据', {
            'fields': ('created_by', 'created_at'),
            'classes': ('collapse',)
        }),
    )

    def status_badge(self, obj):
        color_map = {
            'pending': 'gray',
            'running': 'blue',
            'success': 'green',
            'failed': 'red',
            'canceled': 'orange'
        }
        color = color_map.get(obj.status, 'gray')
        return format_html(
            '<span style="color: {}; font-weight: bold;">{}</span>',
            color,
            obj.get_status_display()
        )
    status_badge.short_description = '状态'

    def save_model(self, request, obj, form, change):
        if not change:
            obj.created_by = request.user
        super().save_model(request, obj, form, change)

    def response_add(self, request, obj, post_url_continue=None):
        if "_run_now" in request.POST:
            task = execute_oneoff_backup_task.delay(obj.id)
            obj.task_id = task.id
            obj.run_at = obj.run_at or timezone.now()
            obj.save(update_fields=['task_id', 'run_at'])
            messages.success(request, f'已创建立即执行任务，任务ID: {task.id}')
            return HttpResponseRedirect(reverse('admin:backups_backuponeofftask_change', args=[obj.id]))

        if obj.status == 'pending' and not obj.task_id:
            task = execute_oneoff_backup_task.apply_async(args=[obj.id], eta=obj.run_at)
            obj.task_id = task.id
            obj.save(update_fields=['task_id'])
            messages.success(request, f'已创建定时任务，任务ID: {task.id}')
        return super().response_add(request, obj, post_url_continue)

    def response_change(self, request, obj):
        if "_run_now" in request.POST:
            task = execute_oneoff_backup_task.delay(obj.id)
            obj.task_id = task.id
            obj.run_at = timezone.now()
            obj.status = 'pending'
            obj.save(update_fields=['task_id', 'run_at', 'status'])
            messages.success(request, f'已创建立即执行任务，任务ID: {task.id}')
            return HttpResponseRedirect(request.path)
        return super().response_change(request, obj)


@admin.register(BackupTaskBoard)
class BackupTaskBoardAdmin(admin.ModelAdmin):
    """任务列表（合并展示周期任务与定时任务）"""

    change_list_template = 'admin/backups/backuptaskboard/change_list.html'

    def has_add_permission(self, request):
        return False

    def has_change_permission(self, request, obj=None):
        return False

    def has_delete_permission(self, request, obj=None):
        return False

    def changelist_view(self, request, extra_context=None):
        tab = request.GET.get('tab', 'pending')
        pending_strategies = BackupStrategy.objects.filter(is_enabled=True).select_related('instance')
        pending_oneoffs = BackupOneOffTask.objects.filter(
            status__in=['pending', 'running']
        ).select_related('instance', 'backup_record')

        executed_records = BackupRecord.objects.filter(
            status__in=['success', 'failed']
        ).select_related('instance', 'strategy').order_by('-created_at')[:200]

        context = {
            **self.admin_site.each_context(request),
            'title': '任务列表',
            'tab': tab,
            'pending_strategies': pending_strategies,
            'pending_oneoffs': pending_oneoffs,
            'executed_records': executed_records,
            'strategy_add_url': reverse('admin:backups_backupstrategy_add'),
            'oneoff_add_url': reverse('admin:backups_backuponeofftask_add'),
            'record_changelist_url': reverse('admin:backups_backuprecord_changelist'),
        }
        if extra_context:
            context.update(extra_context)
        return TemplateResponse(request, self.change_list_template, context)


@admin.register(BackupRestoreBoard)
class BackupRestoreBoardAdmin(admin.ModelAdmin):
    """恢复管理（按备份记录进行恢复）"""

    change_list_template = 'admin/backups/backuprestoreboard/change_list.html'

    def has_add_permission(self, request):
        return False

    def has_change_permission(self, request, obj=None):
        return False

    def has_delete_permission(self, request, obj=None):
        return False

    def changelist_view(self, request, extra_context=None):
        records = BackupRecord.objects.filter(
            status='success'
        ).select_related('instance', 'strategy').order_by('-created_at')[:200]

        context = {
            **self.admin_site.each_context(request),
            'title': '备份恢复',
            'records': records,
        }
        if extra_context:
            context.update(extra_context)
        return TemplateResponse(request, self.change_list_template, context)


for model in (PeriodicTask, CrontabSchedule, IntervalSchedule, SolarSchedule, ClockedSchedule):
    if model:
        try:
            admin.site.unregister(model)
        except admin.sites.NotRegistered:
            pass
