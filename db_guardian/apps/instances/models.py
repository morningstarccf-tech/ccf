"""
MySQL 实例管理模块的数据库模型

包含 MySQL 实例、数据库、监控指标等核心模型。
"""
from django.db import models
from django.conf import settings
from django.utils.translation import gettext_lazy as _
from django.core.exceptions import ValidationError
from cryptography.fernet import Fernet
import pymysql
import logging

logger = logging.getLogger(__name__)


class PasswordEncryptor:
    """密码加密工具类"""
    
    @staticmethod
    def get_cipher():
        """获取加密器实例"""
        key = getattr(settings, 'ENCRYPTION_KEY', Fernet.generate_key().decode()).encode()
        return Fernet(key)
    
    @staticmethod
    def encrypt(password: str) -> str:
        """
        加密密码
        
        Args:
            password: 明文密码
            
        Returns:
            str: 加密后的密码
        """
        if not password:
            return ''
        cipher = PasswordEncryptor.get_cipher()
        return cipher.encrypt(password.encode()).decode()
    
    @staticmethod
    def decrypt(encrypted: str) -> str:
        """
        解密密码
        
        Args:
            encrypted: 加密的密码
            
        Returns:
            str: 明文密码
        """
        if not encrypted:
            return ''
        cipher = PasswordEncryptor.get_cipher()
        return cipher.decrypt(encrypted.encode()).decode()


class MySQLInstance(models.Model):
    """
    MySQL 实例模型
    
    用于管理 MySQL 数据库实例的连接信息和状态。
    密码使用 Fernet 加密存储。
    """
    
    STATUS_CHOICES = [
        ('online', _('在线')),
        ('offline', _('离线')),
        ('error', _('错误')),
    ]

    DEPLOYMENT_CHOICES = [
        ('docker', _('Docker容器')),
        ('systemd', _('系统服务')),
    ]
    
    alias = models.CharField(
        _('实例别名'),
        max_length=100,
        unique=True,
        help_text=_('实例的显示名称，必须唯一')
    )
    
    host = models.CharField(
        _('主机地址'),
        max_length=255,
        help_text=_('MySQL 服务器的 IP 地址或域名')
    )
    
    port = models.PositiveIntegerField(
        _('端口号'),
        default=3306,
        help_text=_('MySQL 服务器的端口号')
    )
    
    username = models.CharField(
        _('用户名'),
        max_length=100,
        help_text=_('连接 MySQL 的用户名')
    )
    
    password = models.TextField(
        _('密码'),
        help_text=_('加密存储的密码')
    )

    deployment_type = models.CharField(
        _('部署方式'),
        max_length=20,
        choices=DEPLOYMENT_CHOICES,
        default='systemd',
        help_text=_('MySQL 实例运行方式（Docker 容器或系统服务）')
    )

    docker_container_name = models.CharField(
        _('容器名称'),
        max_length=200,
        blank=True,
        help_text=_('Docker 部署时的容器名称')
    )

    mysql_service_name = models.CharField(
        _('服务名称'),
        max_length=200,
        default='mysql',
        help_text=_('系统服务名称（systemd），如 mysql 或 mysqld')
    )

    data_dir = models.CharField(
        _('数据目录'),
        max_length=500,
        blank=True,
        help_text=_('MySQL 数据目录（用于冷备/热备物理备份）')
    )

    ssh_host = models.CharField(
        _('SSH 主机'),
        max_length=255,
        blank=True,
        help_text=_('执行备份命令的远程主机地址')
    )

    ssh_port = models.PositiveIntegerField(
        _('SSH 端口'),
        default=22,
        help_text=_('SSH 连接端口')
    )

    ssh_user = models.CharField(
        _('SSH 用户'),
        max_length=100,
        blank=True,
        help_text=_('SSH 用户名')
    )

    ssh_password = models.TextField(
        _('SSH 密码'),
        blank=True,
        help_text=_('加密存储的 SSH 密码')
    )

    ssh_key_path = models.CharField(
        _('SSH 私钥路径'),
        max_length=500,
        blank=True,
        help_text=_('SSH 私钥路径（优先于密码）')
    )

    xtrabackup_bin = models.CharField(
        _('XtraBackup 路径'),
        max_length=300,
        default='xtrabackup',
        help_text=_('xtrabackup 可执行文件路径或命令名')
    )
    
    team = models.ForeignKey(
        'authentication.Team',
        on_delete=models.CASCADE,
        related_name='mysql_instances',
        verbose_name=_('所属团队'),
        help_text=_('该实例所属的团队')
    )
    
    description = models.TextField(
        _('描述'),
        blank=True,
        help_text=_('实例的详细说明')
    )
    
    status = models.CharField(
        _('状态'),
        max_length=20,
        choices=STATUS_CHOICES,
        default='offline',
        help_text=_('实例当前的运行状态')
    )
    
    last_check_time = models.DateTimeField(
        _('最后检查时间'),
        null=True,
        blank=True,
        help_text=_('最后一次健康检查的时间')
    )
    
    version = models.CharField(
        _('MySQL 版本'),
        max_length=50,
        blank=True,
        help_text=_('MySQL 服务器版本号')
    )
    
    charset = models.CharField(
        _('字符集'),
        max_length=50,
        default='utf8mb4',
        help_text=_('数据库默认字符集')
    )
    
    created_by = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.SET_NULL,
        null=True,
        related_name='created_instances',
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
    
    class Meta:
        db_table = 'mysql_instance'
        verbose_name = _('MySQL 实例')
        verbose_name_plural = _('MySQL 实例')
        ordering = ['-created_at']
        indexes = [
            models.Index(fields=['team'], name='idx_instance_team'),
            models.Index(fields=['status'], name='idx_instance_status'),
            models.Index(fields=['alias'], name='idx_instance_alias'),
        ]
    
    def __str__(self):
        return f"{self.alias} ({self.host}:{self.port})"
    
    def save(self, *args, **kwargs):
        """重写 save 方法，自动加密密码"""
        # 如果密码已更改且不是加密格式，则加密
        if self.pk:
            # 更新时检查密码是否变化
            old_instance = MySQLInstance.objects.filter(pk=self.pk).first()
            if old_instance and old_instance.password != self.password:
                # 密码已更改，需要重新加密
                self.password = PasswordEncryptor.encrypt(self.password)
            if old_instance and old_instance.ssh_password != self.ssh_password:
                self.ssh_password = PasswordEncryptor.encrypt(self.ssh_password)
        else:
            # 新建时加密密码
            self.password = PasswordEncryptor.encrypt(self.password)
            self.ssh_password = PasswordEncryptor.encrypt(self.ssh_password)
        
        super().save(*args, **kwargs)
    
    def get_decrypted_password(self) -> str:
        """
        获取解密后的密码
        
        Returns:
            str: 明文密码
        """
        return PasswordEncryptor.decrypt(self.password)

    def get_decrypted_ssh_password(self) -> str:
        """
        获取解密后的 SSH 密码

        Returns:
            str: 明文密码
        """
        return PasswordEncryptor.decrypt(self.ssh_password)
    
    def test_connection(self) -> tuple[bool, str]:
        """
        测试数据库连接
        
        Returns:
            tuple: (是否成功, 消息)
        """
        try:
            connection = pymysql.connect(
                host=self.host,
                port=self.port,
                user=self.username,
                password=self.get_decrypted_password(),
                connect_timeout=5
            )
            
            # 获取版本信息
            with connection.cursor() as cursor:
                cursor.execute("SELECT VERSION()")
                version = cursor.fetchone()[0]
            
            connection.close()
            return True, f"连接成功，MySQL 版本: {version}"
        
        except pymysql.Error as e:
            error_msg = f"连接失败: {str(e)}"
            logger.error(f"Instance {self.alias} connection test failed: {error_msg}")
            return False, error_msg
        
        except Exception as e:
            error_msg = f"未知错误: {str(e)}"
            logger.exception(f"Instance {self.alias} connection test error: {error_msg}")
            return False, error_msg
    
    def get_connection(self):
        """
        获取数据库连接
        
        Returns:
            pymysql.Connection: 数据库连接对象
            
        Raises:
            pymysql.Error: 连接失败时抛出异常
        """
        return pymysql.connect(
            host=self.host,
            port=self.port,
            user=self.username,
            password=self.get_decrypted_password(),
            charset=self.charset,
            cursorclass=pymysql.cursors.DictCursor
        )
    
    def get_version(self) -> str:
        """
        获取 MySQL 版本
        
        Returns:
            str: MySQL 版本号
        """
        try:
            connection = self.get_connection()
            with connection.cursor() as cursor:
                cursor.execute("SELECT VERSION()")
                version = cursor.fetchone()['VERSION()']
            connection.close()
            return version
        except Exception as e:
            logger.error(f"Failed to get version for {self.alias}: {str(e)}")
            return ''
    
    def update_status(self):
        """更新实例状态"""
        from django.utils import timezone
        
        success, message = self.test_connection()
        if success:
            self.status = 'online'
            # 更新版本信息
            if not self.version:
                self.version = self.get_version()
        else:
            self.status = 'error' if 'timeout' not in message.lower() else 'offline'
        
        self.last_check_time = timezone.now()
        # 使用 update_fields 避免触发 save 方法中的密码加密逻辑
        MySQLInstance.objects.filter(pk=self.pk).update(
            status=self.status,
            last_check_time=self.last_check_time,
            version=self.version
        )


class Database(models.Model):
    """
    数据库模型
    
    表示 MySQL 实例中的一个数据库。
    """
    
    instance = models.ForeignKey(
        MySQLInstance,
        on_delete=models.CASCADE,
        related_name='databases',
        verbose_name=_('MySQL 实例')
    )
    
    name = models.CharField(
        _('数据库名称'),
        max_length=100,
        help_text=_('数据库的名称')
    )
    
    charset = models.CharField(
        _('字符集'),
        max_length=50,
        default='utf8mb4',
        help_text=_('数据库字符集')
    )
    
    collation = models.CharField(
        _('排序规则'),
        max_length=50,
        default='utf8mb4_unicode_ci',
        help_text=_('数据库排序规则')
    )
    
    size_mb = models.FloatField(
        _('数据库大小(MB)'),
        default=0,
        help_text=_('数据库占用的磁盘空间大小')
    )
    
    table_count = models.IntegerField(
        _('表数量'),
        default=0,
        help_text=_('数据库中的表数量')
    )
    
    last_backup_time = models.DateTimeField(
        _('最后备份时间'),
        null=True,
        blank=True,
        help_text=_('最后一次成功备份的时间')
    )
    
    created_at = models.DateTimeField(
        _('创建时间'),
        auto_now_add=True
    )
    
    updated_at = models.DateTimeField(
        _('更新时间'),
        auto_now=True
    )
    
    class Meta:
        db_table = 'mysql_database'
        verbose_name = _('数据库')
        verbose_name_plural = _('数据库')
        ordering = ['instance', 'name']
        unique_together = [['instance', 'name']]
        indexes = [
            models.Index(fields=['instance', 'name'], name='idx_db_instance_name'),
        ]
    
    def __str__(self):
        return f"{self.instance.alias}.{self.name}"
    
    def update_statistics(self):
        """更新数据库统计信息（大小和表数量）"""
        try:
            connection = self.instance.get_connection()
            
            with connection.cursor() as cursor:
                # 获取数据库大小
                cursor.execute("""
                    SELECT 
                        ROUND(SUM(data_length + index_length) / 1024 / 1024, 2) as size_mb,
                        COUNT(*) as table_count
                    FROM information_schema.TABLES
                    WHERE table_schema = %s
                """, (self.name,))
                
                result = cursor.fetchone()
                self.size_mb = result['size_mb'] or 0
                self.table_count = result['table_count'] or 0
            
            connection.close()
            self.save()
            
        except Exception as e:
            logger.error(f"Failed to update statistics for {self}: {str(e)}")


class MonitoringMetrics(models.Model):
    """
    监控指标模型
    
    记录 MySQL 实例的性能监控数据。
    """
    
    instance = models.ForeignKey(
        MySQLInstance,
        on_delete=models.CASCADE,
        related_name='metrics',
        verbose_name=_('MySQL 实例')
    )
    
    timestamp = models.DateTimeField(
        _('时间戳'),
        auto_now_add=True,
        help_text=_('指标采集的时间')
    )
    
    qps = models.FloatField(
        _('每秒查询数'),
        default=0,
        help_text=_('Queries Per Second')
    )
    
    tps = models.FloatField(
        _('每秒事务数'),
        default=0,
        help_text=_('Transactions Per Second')
    )
    
    connections = models.IntegerField(
        _('当前连接数'),
        default=0,
        help_text=_('当前活跃的数据库连接数')
    )
    
    slow_queries = models.IntegerField(
        _('慢查询数'),
        default=0,
        help_text=_('慢查询的累计数量')
    )
    
    cpu_usage = models.FloatField(
        _('CPU 使用率'),
        default=0,
        help_text=_('CPU 使用百分比')
    )
    
    memory_usage = models.FloatField(
        _('内存使用率'),
        default=0,
        help_text=_('内存使用百分比')
    )
    
    disk_usage = models.FloatField(
        _('磁盘使用率'),
        default=0,
        help_text=_('磁盘使用百分比')
    )
    
    class Meta:
        db_table = 'mysql_monitoring_metrics'
        verbose_name = _('监控指标')
        verbose_name_plural = _('监控指标')
        ordering = ['-timestamp']
        indexes = [
            models.Index(fields=['instance', '-timestamp'], name='idx_metrics_instance_time'),
        ]
    
    def __str__(self):
        return f"{self.instance.alias} - {self.timestamp}"
