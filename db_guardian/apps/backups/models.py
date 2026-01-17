"""
备份管理模块的数据库模型

包含备份策略、备份记录等核心模型。
"""
from django.db import models
from django.conf import settings
from django.utils.translation import gettext_lazy as _
from django.core.validators import MinValueValidator
from apps.instances.models import PasswordEncryptor
import logging

logger = logging.getLogger(__name__)


class BackupStrategy(models.Model):
    """
    备份策略模型
    
    定义自动备份的规则和配置，包括备份周期、保留策略等。
    """
    
    BACKUP_TYPE_CHOICES = [
        ('full', _('全量备份')),
        ('incremental', _('增量备份')),
        ('hot', _('热备份')),
        ('cold', _('冷备份')),
    ]

    STORAGE_MODE_CHOICES = [
        ('default', _('默认容器路径')),
        ('mysql_host', _('MySQL 服务器路径')),
        ('remote_server', _('远程服务器路径')),
        ('oss', _('云存储（OSS）')),
    ]

    REMOTE_PROTOCOL_CHOICES = [
        ('ssh', _('SSH')),
        ('ftp', _('FTP')),
        ('http', _('HTTP')),
    ]
    
    name = models.CharField(
        _('策略名称'),
        max_length=100,
        help_text=_('备份策略的显示名称')
    )
    
    instance = models.ForeignKey(
        'instances.MySQLInstance',
        on_delete=models.CASCADE,
        related_name='backup_strategies',
        verbose_name=_('MySQL 实例'),
        help_text=_('要备份的 MySQL 实例')
    )
    
    databases = models.JSONField(
        _('数据库列表'),
        blank=True,
        null=True,
        help_text=_('要备份的数据库列表，为空表示备份所有数据库')
    )
    
    cron_expression = models.CharField(
        _('Cron 表达式'),
        max_length=100,
        help_text=_('备份执行的 Cron 表达式，如 "0 2 * * *" 表示每天凌晨2点')
    )
    
    backup_type = models.CharField(
        _('备份类型'),
        max_length=20,
        choices=BACKUP_TYPE_CHOICES,
        default='full',
        help_text=_('备份类型：全量、增量、热备或冷备')
    )
    
    retention_days = models.IntegerField(
        _('保留天数'),
        default=7,
        validators=[MinValueValidator(1)],
        help_text=_('备份文件保留的天数')
    )
    
    is_enabled = models.BooleanField(
        _('是否启用'),
        default=True,
        help_text=_('是否启用该备份策略')
    )
    
    storage_path = models.CharField(
        _('存储路径'),
        max_length=500,
        blank=True,
        help_text=_('备份文件的存储路径，为空则使用默认路径')
    )

    storage_mode = models.CharField(
        _('存储位置'),
        max_length=20,
        choices=STORAGE_MODE_CHOICES,
        default='default',
        help_text=_('备份文件的存储位置')
    )

    store_local = models.BooleanField(
        _('本地保存'),
        default=True,
        help_text=_('将备份保存到本地存储路径')
    )

    store_remote = models.BooleanField(
        _('远程保存'),
        default=False,
        help_text=_('通过 SSH 保存到远程服务器目录')
    )

    store_oss = models.BooleanField(
        _('云存储保存'),
        default=False,
        help_text=_('上传到对象存储（如 OSS）')
    )

    remote_storage_path = models.CharField(
        _('远程存储路径'),
        max_length=500,
        blank=True,
        help_text=_('远程服务器存储路径（优先于实例的远程备份目录）')
    )

    remote_protocol = models.CharField(
        _('远程协议'),
        max_length=10,
        choices=REMOTE_PROTOCOL_CHOICES,
        blank=True,
        help_text=_('远程服务器传输协议')
    )

    remote_host = models.CharField(
        _('远程主机'),
        max_length=255,
        blank=True,
        help_text=_('远程服务器地址')
    )

    remote_port = models.PositiveIntegerField(
        _('远程端口'),
        null=True,
        blank=True,
        help_text=_('远程服务器端口')
    )

    remote_user = models.CharField(
        _('远程用户名'),
        max_length=100,
        blank=True,
        help_text=_('远程服务器用户名')
    )

    remote_password = models.TextField(
        _('远程密码'),
        blank=True,
        help_text=_('加密存储的远程服务器密码')
    )

    remote_key_path = models.CharField(
        _('远程密钥路径'),
        max_length=500,
        blank=True,
        help_text=_('远程服务器私钥路径（优先于密码）')
    )

    oss_endpoint = models.CharField(
        _('OSS Endpoint'),
        max_length=255,
        blank=True,
        help_text=_('对象存储 Endpoint')
    )

    oss_access_key_id = models.CharField(
        _('OSS AccessKey'),
        max_length=255,
        blank=True,
        help_text=_('对象存储 AccessKey ID')
    )

    oss_access_key_secret = models.TextField(
        _('OSS AccessKey Secret'),
        blank=True,
        help_text=_('加密存储的对象存储密钥')
    )

    oss_bucket = models.CharField(
        _('OSS Bucket'),
        max_length=255,
        blank=True,
        help_text=_('对象存储 Bucket 名称')
    )

    oss_prefix = models.CharField(
        _('OSS 路径'),
        max_length=255,
        blank=True,
        help_text=_('对象存储路径前缀')
    )
    
    compress = models.BooleanField(
        _('是否压缩'),
        default=True,
        help_text=_('是否压缩备份文件')
    )
    
    created_by = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.SET_NULL,
        null=True,
        related_name='created_backup_strategies',
        verbose_name=_('创建者')
    )
    
    created_at = models.DateTimeField(
        _('创建时间'),
        auto_now_add=True
    )
    
    updated_at = models.DateTimeField(
        _('更新时间'),
        auto_now=True
    )

    def save(self, *args, **kwargs):
        if self.pk:
            old = BackupStrategy.objects.filter(pk=self.pk).only(
                'remote_password', 'oss_access_key_secret'
            ).first()
            if old and old.remote_password != self.remote_password:
                if self.remote_password:
                    self.remote_password = PasswordEncryptor.encrypt(self.remote_password)
            if old and old.oss_access_key_secret != self.oss_access_key_secret:
                if self.oss_access_key_secret:
                    self.oss_access_key_secret = PasswordEncryptor.encrypt(self.oss_access_key_secret)
        else:
            if self.remote_password:
                self.remote_password = PasswordEncryptor.encrypt(self.remote_password)
            if self.oss_access_key_secret:
                self.oss_access_key_secret = PasswordEncryptor.encrypt(self.oss_access_key_secret)

        super().save(*args, **kwargs)

    def get_decrypted_remote_password(self) -> str:
        if not self.remote_password:
            return ''
        try:
            return PasswordEncryptor.decrypt(self.remote_password)
        except Exception:
            return self.remote_password

    def get_decrypted_oss_access_key_secret(self) -> str:
        if not self.oss_access_key_secret:
            return ''
        try:
            return PasswordEncryptor.decrypt(self.oss_access_key_secret)
        except Exception:
            return self.oss_access_key_secret
    
    class Meta:
        db_table = 'backup_strategy'
        verbose_name = _('备份策略')
        verbose_name_plural = _('备份策略')
        ordering = ['-created_at']
        indexes = [
            models.Index(fields=['instance'], name='idx_strategy_instance'),
            models.Index(fields=['is_enabled'], name='idx_strategy_enabled'),
        ]
    
    def __str__(self):
        return f"{self.name} - {self.instance.alias}"
    
    def get_storage_path(self):
        """
        获取备份存储路径
        
        Returns:
            str: 存储路径
        """
        if self.storage_path:
            return self.storage_path
        # 使用默认路径：backups/instance_alias/
        from pathlib import Path
        backup_root = getattr(settings, 'BACKUP_STORAGE_PATH', settings.BASE_DIR / 'backups')
        return str(Path(backup_root) / self.instance.alias)

    def get_schedule_display(self):
        """获取策略调度描述（默认使用 Cron 表达式）"""
        cron_expr = (self.cron_expression or '').strip()
        parts = cron_expr.split()
        if len(parts) != 5:
            return cron_expr or '-'

        minute, hour, day_of_month, _month_of_year, day_of_week = parts
        if day_of_month == '*' and day_of_week == '*':
            if hour == '*':
                if minute.isdigit():
                    return f"每小时 {int(minute):02d} 分"
                return cron_expr
            if hour.isdigit() and minute.isdigit():
                return f"每天 {int(hour):02d}:{int(minute):02d}"
            return cron_expr

        weekday_map = {
            '1': '周一',
            '2': '周二',
            '3': '周三',
            '4': '周四',
            '5': '周五',
            '6': '周六',
            '0': '周日',
        }
        if day_of_month == '*' and day_of_week in weekday_map:
            if hour.isdigit() and minute.isdigit():
                return f"每周{weekday_map[day_of_week]} {int(hour):02d}:{int(minute):02d}"
            return cron_expr

        if day_of_month.isdigit() and day_of_week == '*':
            if hour.isdigit() and minute.isdigit():
                return f"每月{int(day_of_month)}日 {int(hour):02d}:{int(minute):02d}"
            return cron_expr

        return cron_expr


class BackupRecord(models.Model):
    """
    备份记录模型
    
    记录每次备份的详细信息，包括状态、文件路径、大小等。
    """
    
    STATUS_CHOICES = [
        ('pending', _('等待中')),
        ('running', _('执行中')),
        ('success', _('成功')),
        ('failed', _('失败')),
    ]
    
    BACKUP_TYPE_CHOICES = [
        ('full', _('全量备份')),
        ('incremental', _('增量备份')),
        ('hot', _('热备份')),
        ('cold', _('冷备份')),
    ]
    
    instance = models.ForeignKey(
        'instances.MySQLInstance',
        on_delete=models.CASCADE,
        related_name='backup_records',
        verbose_name=_('MySQL 实例'),
        help_text=_('备份的 MySQL 实例')
    )
    
    strategy = models.ForeignKey(
        BackupStrategy,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name='backup_records',
        verbose_name=_('备份策略'),
        help_text=_('关联的备份策略，手动备份时为空')
    )
    
    database_name = models.CharField(
        _('数据库名称'),
        max_length=100,
        blank=True,
        help_text=_('备份的数据库名称，为空表示全实例备份')
    )
    
    backup_type = models.CharField(
        _('备份类型'),
        max_length=20,
        choices=BACKUP_TYPE_CHOICES,
        default='full',
        help_text=_('备份类型：全量、增量、热备或冷备')
    )

    base_backup = models.ForeignKey(
        'self',
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name='incremental_children',
        verbose_name=_('基准备份'),
        help_text=_('增量备份的基准备份记录')
    )
    
    status = models.CharField(
        _('状态'),
        max_length=20,
        choices=STATUS_CHOICES,
        default='pending',
        help_text=_('备份任务的执行状态')
    )
    
    file_path = models.CharField(
        _('文件路径'),
        max_length=500,
        blank=True,
        help_text=_('备份文件的完整路径')
    )

    remote_path = models.CharField(
        _('远程路径'),
        max_length=500,
        blank=True,
        default='',
        help_text=_('远程服务器备份路径')
    )

    remote_protocol = models.CharField(
        _('远程协议'),
        max_length=10,
        choices=BackupStrategy.REMOTE_PROTOCOL_CHOICES,
        blank=True,
        help_text=_('远程服务器传输协议')
    )

    remote_host = models.CharField(
        _('远程主机'),
        max_length=255,
        blank=True,
        help_text=_('远程服务器地址')
    )

    remote_port = models.PositiveIntegerField(
        _('远程端口'),
        null=True,
        blank=True,
        help_text=_('远程服务器端口')
    )

    remote_user = models.CharField(
        _('远程用户名'),
        max_length=100,
        blank=True,
        help_text=_('远程服务器用户名')
    )

    remote_password = models.TextField(
        _('远程密码'),
        blank=True,
        help_text=_('加密存储的远程服务器密码')
    )

    remote_key_path = models.CharField(
        _('远程密钥路径'),
        max_length=500,
        blank=True,
        help_text=_('远程服务器私钥路径（优先于密码）')
    )

    object_storage_path = models.CharField(
        _('对象存储路径'),
        max_length=500,
        blank=True,
        default='',
        help_text=_('对象存储路径（如 OSS）')
    )
    
    file_size_mb = models.FloatField(
        _('文件大小(MB)'),
        default=0,
        help_text=_('备份文件的大小（MB）')
    )
    
    start_time = models.DateTimeField(
        _('开始时间'),
        null=True,
        blank=True,
        help_text=_('备份任务开始的时间')
    )
    
    end_time = models.DateTimeField(
        _('结束时间'),
        null=True,
        blank=True,
        help_text=_('备份任务结束的时间')
    )
    
    error_message = models.TextField(
        _('错误信息'),
        blank=True,
        help_text=_('备份失败时的错误详情')
    )
    
    created_by = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name='created_backup_records',
        verbose_name=_('创建者'),
        help_text=_('手动触发备份的用户')
    )
    
    created_at = models.DateTimeField(
        _('创建时间'),
        auto_now_add=True
    )
    
    class Meta:
        db_table = 'backup_record'
        verbose_name = _('备份记录')
        verbose_name_plural = _('备份记录')
        ordering = ['-created_at']
        indexes = [
            models.Index(fields=['instance', 'status'], name='idx_record_instance_status'),
            models.Index(fields=['instance', '-start_time'], name='idx_record_instance_time'),
            models.Index(fields=['status'], name='idx_record_status'),
        ]
    
    def __str__(self):
        db_info = self.database_name or '全实例'
        return f"{self.instance.alias} - {db_info} - {self.get_status_display()}"
    
    def get_duration_seconds(self):
        """
        获取备份耗时（秒）
        
        Returns:
            float: 耗时秒数，如果未完成则返回 None
        """
        if self.start_time and self.end_time:
            return (self.end_time - self.start_time).total_seconds()
        return None

    def get_decrypted_remote_password(self) -> str:
        if not self.remote_password:
            return ''
        try:
            return PasswordEncryptor.decrypt(self.remote_password)
        except Exception:
            return self.remote_password


class BackupOneOffTask(models.Model):
    """
    定时备份任务（一次性）
    """

    STATUS_CHOICES = [
        ('pending', _('等待中')),
        ('running', _('执行中')),
        ('success', _('成功')),
        ('failed', _('失败')),
        ('canceled', _('已取消')),
    ]

    name = models.CharField(
        _('任务名称'),
        max_length=120,
        help_text=_('一次性定时任务名称')
    )

    instance = models.ForeignKey(
        'instances.MySQLInstance',
        on_delete=models.CASCADE,
        related_name='oneoff_backup_tasks',
        verbose_name=_('MySQL 实例'),
        help_text=_('要备份的 MySQL 实例')
    )

    databases = models.JSONField(
        _('数据库列表'),
        blank=True,
        null=True,
        help_text=_('要备份的数据库列表，为空表示备份所有数据库')
    )

    backup_type = models.CharField(
        _('备份类型'),
        max_length=20,
        choices=BackupStrategy.BACKUP_TYPE_CHOICES,
        default='full',
        help_text=_('备份类型：全量、增量、热备或冷备')
    )

    run_at = models.DateTimeField(
        _('执行时间'),
        help_text=_('任务计划执行时间')
    )

    compress = models.BooleanField(
        _('是否压缩'),
        default=True,
        help_text=_('是否压缩备份文件')
    )

    storage_path = models.CharField(
        _('存储路径'),
        max_length=500,
        blank=True,
        help_text=_('备份文件的存储路径，为空则使用默认路径')
    )

    storage_mode = models.CharField(
        _('存储位置'),
        max_length=20,
        choices=BackupStrategy.STORAGE_MODE_CHOICES,
        default='default',
        help_text=_('备份文件的存储位置')
    )

    store_local = models.BooleanField(
        _('本地保存'),
        default=True,
        help_text=_('将备份保存到本地存储路径')
    )

    store_remote = models.BooleanField(
        _('远程保存'),
        default=False,
        help_text=_('通过 SSH 保存到远程服务器目录')
    )

    store_oss = models.BooleanField(
        _('云存储保存'),
        default=False,
        help_text=_('上传到对象存储（如 OSS）')
    )

    remote_storage_path = models.CharField(
        _('远程存储路径'),
        max_length=500,
        blank=True,
        help_text=_('远程服务器存储路径（优先于实例的远程备份目录）')
    )

    remote_protocol = models.CharField(
        _('远程协议'),
        max_length=10,
        choices=BackupStrategy.REMOTE_PROTOCOL_CHOICES,
        blank=True,
        help_text=_('远程服务器传输协议')
    )

    remote_host = models.CharField(
        _('远程主机'),
        max_length=255,
        blank=True,
        help_text=_('远程服务器地址')
    )

    remote_port = models.PositiveIntegerField(
        _('远程端口'),
        null=True,
        blank=True,
        help_text=_('远程服务器端口')
    )

    remote_user = models.CharField(
        _('远程用户名'),
        max_length=100,
        blank=True,
        help_text=_('远程服务器用户名')
    )

    remote_password = models.TextField(
        _('远程密码'),
        blank=True,
        help_text=_('加密存储的远程服务器密码')
    )

    remote_key_path = models.CharField(
        _('远程密钥路径'),
        max_length=500,
        blank=True,
        help_text=_('远程服务器私钥路径（优先于密码）')
    )

    oss_endpoint = models.CharField(
        _('OSS Endpoint'),
        max_length=255,
        blank=True,
        help_text=_('对象存储 Endpoint')
    )

    oss_access_key_id = models.CharField(
        _('OSS AccessKey'),
        max_length=255,
        blank=True,
        help_text=_('对象存储 AccessKey ID')
    )

    oss_access_key_secret = models.TextField(
        _('OSS AccessKey Secret'),
        blank=True,
        help_text=_('加密存储的对象存储密钥')
    )

    oss_bucket = models.CharField(
        _('OSS Bucket'),
        max_length=255,
        blank=True,
        help_text=_('对象存储 Bucket 名称')
    )

    oss_prefix = models.CharField(
        _('OSS 路径'),
        max_length=255,
        blank=True,
        help_text=_('对象存储路径前缀')
    )

    status = models.CharField(
        _('状态'),
        max_length=20,
        choices=STATUS_CHOICES,
        default='pending',
        help_text=_('任务执行状态')
    )

    task_id = models.CharField(
        _('Celery 任务ID'),
        max_length=100,
        blank=True,
        help_text=_('Celery 调度的任务ID')
    )

    backup_record = models.ForeignKey(
        BackupRecord,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name='oneoff_tasks',
        verbose_name=_('关联备份记录')
    )

    error_message = models.TextField(
        _('错误信息'),
        blank=True,
        help_text=_('失败原因')
    )

    created_by = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name='created_oneoff_tasks',
        verbose_name=_('创建者')
    )

    created_at = models.DateTimeField(
        _('创建时间'),
        auto_now_add=True
    )

    def save(self, *args, **kwargs):
        if self.pk:
            old = BackupOneOffTask.objects.filter(pk=self.pk).only(
                'remote_password', 'oss_access_key_secret'
            ).first()
            if old and old.remote_password != self.remote_password:
                if self.remote_password:
                    self.remote_password = PasswordEncryptor.encrypt(self.remote_password)
            if old and old.oss_access_key_secret != self.oss_access_key_secret:
                if self.oss_access_key_secret:
                    self.oss_access_key_secret = PasswordEncryptor.encrypt(self.oss_access_key_secret)
        else:
            if self.remote_password:
                self.remote_password = PasswordEncryptor.encrypt(self.remote_password)
            if self.oss_access_key_secret:
                self.oss_access_key_secret = PasswordEncryptor.encrypt(self.oss_access_key_secret)

        super().save(*args, **kwargs)

    def get_decrypted_remote_password(self) -> str:
        if not self.remote_password:
            return ''
        try:
            return PasswordEncryptor.decrypt(self.remote_password)
        except Exception:
            return self.remote_password

    def get_decrypted_oss_access_key_secret(self) -> str:
        if not self.oss_access_key_secret:
            return ''
        try:
            return PasswordEncryptor.decrypt(self.oss_access_key_secret)
        except Exception:
            return self.oss_access_key_secret

    started_at = models.DateTimeField(
        _('开始时间'),
        null=True,
        blank=True
    )

    finished_at = models.DateTimeField(
        _('结束时间'),
        null=True,
        blank=True
    )

    class Meta:
        db_table = 'backup_oneoff_task'
        verbose_name = _('定时任务')
        verbose_name_plural = _('定时任务')
        ordering = ['-run_at']
        indexes = [
            models.Index(fields=['status'], name='idx_oneoff_status'),
            models.Index(fields=['instance', 'run_at'], name='idx_oneoff_instance_time'),
        ]

    def __str__(self):
        return f"{self.name} - {self.instance.alias}"


class BackupTaskBoard(BackupRecord):
    """备份任务总览（代理模型）"""

    class Meta:
        proxy = True
        verbose_name = _('任务列表')
        verbose_name_plural = _('任务列表')


class BackupRestoreBoard(BackupRecord):
    """备份恢复（代理模型）"""

    class Meta:
        proxy = True
        verbose_name = _('备份恢复')
        verbose_name_plural = _('备份恢复')

