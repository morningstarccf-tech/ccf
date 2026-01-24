"""
SQL客户端应用数据库模型

提供 SQL 执行历史记录和查询结果管理功能。
"""
from django.db import models
from django.conf import settings
from django.utils.translation import gettext_lazy as _
from apps.instances.models import MySQLInstance


class QueryHistory(models.Model):
    """
    SQL 查询执行历史模型
    
    记录所有 SQL 查询的执行历史，包括执行结果、性能指标和错误信息。
    """
    
    SQL_TYPE_CHOICES = [
        ('SELECT', _('查询')),
        ('INSERT', _('插入')),
        ('UPDATE', _('更新')),
        ('DELETE', _('删除')),
        ('DDL', _('数据定义')),
        ('SHOW', _('显示')),
        ('DESC', _('描述')),
        ('EXPLAIN', _('解释')),
        ('OTHER', _('其他')),
    ]
    
    STATUS_CHOICES = [
        ('success', _('成功')),
        ('failed', _('失败')),
        ('timeout', _('超时')),
    ]
    
    instance = models.ForeignKey(
        MySQLInstance,
        on_delete=models.CASCADE,
        related_name='query_histories',
        verbose_name=_('MySQL 实例'),
        help_text=_('执行查询的 MySQL 实例')
    )
    
    database_name = models.CharField(
        _('数据库名称'),
        max_length=100,
        blank=True,
        help_text=_('执行查询的数据库名称，为空表示未指定数据库')
    )
    
    sql_statement = models.TextField(
        _('SQL 语句'),
        help_text=_('执行的 SQL 语句原文')
    )
    
    sql_type = models.CharField(
        _('SQL 类型'),
        max_length=20,
        choices=SQL_TYPE_CHOICES,
        default='OTHER',
        help_text=_('SQL 语句的类型')
    )
    
    status = models.CharField(
        _('执行状态'),
        max_length=20,
        choices=STATUS_CHOICES,
        default='success',
        help_text=_('SQL 执行的状态')
    )
    
    rows_affected = models.IntegerField(
        _('影响行数'),
        default=0,
        help_text=_('SQL 执行影响的行数（INSERT/UPDATE/DELETE）或返回的行数（SELECT）')
    )
    
    execution_time_ms = models.IntegerField(
        _('执行时间(毫秒)'),
        default=0,
        help_text=_('SQL 执行所用的时间，单位为毫秒')
    )
    
    error_message = models.TextField(
        _('错误信息'),
        blank=True,
        help_text=_('SQL 执行失败时的错误信息')
    )
    
    result_cached = models.BooleanField(
        _('结果已缓存'),
        default=False,
        help_text=_('查询结果是否已缓存到 Redis')
    )
    
    result_cache_key = models.CharField(
        _('结果缓存键'),
        max_length=255,
        blank=True,
        help_text=_('Redis 中存储结果的缓存键')
    )
    
    executed_by = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.SET_NULL,
        null=True,
        related_name='sql_queries',
        verbose_name=_('执行者'),
        help_text=_('执行该 SQL 的用户')
    )
    
    executed_at = models.DateTimeField(
        _('执行时间'),
        auto_now_add=True,
        help_text=_('SQL 执行的时间戳')
    )
    
    class Meta:
        db_table = 'sqlclient_query_history'
        verbose_name = _('SQL 执行历史')
        verbose_name_plural = _('SQL 执行历史')
        ordering = ['-executed_at']
        indexes = [
            models.Index(fields=['instance', 'executed_by', '-executed_at'], 
                        name='idx_qh_inst_user_time'),
            models.Index(fields=['instance', '-executed_at'], 
                        name='idx_qh_inst_time'),
            models.Index(fields=['executed_by', '-executed_at'], 
                        name='idx_qh_user_time'),
            models.Index(fields=['status'], 
                        name='idx_qh_status'),
            models.Index(fields=['sql_type'], 
                        name='idx_qh_sql_type'),
        ]
    
    def __str__(self):
        return f"{self.sql_type} @ {self.instance.alias} by {self.executed_by.username} at {self.executed_at}"
    
    def get_result_from_cache(self):
        """
        从 Redis 缓存中获取查询结果
        
        Returns:
            dict: 查询结果数据，如果未缓存或已过期则返回 None
        """
        if not self.result_cached or not self.result_cache_key:
            return None
        
        from django.core.cache import cache
        # 缓存内容是列名/数据等组成的轻量字典。
        return cache.get(self.result_cache_key)
    
    def cache_result(self, result_data, timeout=3600):
        """
        缓存查询结果到 Redis
        
        Args:
            result_data: 查询结果数据
            timeout: 缓存过期时间（秒），默认 1 小时
        """
        from django.core.cache import cache
        import uuid
        
        # 生成缓存键
        cache_key = f"query_result:{uuid.uuid4()}"
        
        # 存储到缓存
        cache.set(cache_key, result_data, timeout)
        
        # 更新记录
        self.result_cached = True
        self.result_cache_key = cache_key
        self.save(update_fields=['result_cached', 'result_cache_key'])


class SQLTerminal(QueryHistory):
    """SQL 终端（使用 QueryHistory 作为代理模型）"""

    class Meta:
        proxy = True
        verbose_name = _('SQL终端')
        verbose_name_plural = _('SQL终端')
