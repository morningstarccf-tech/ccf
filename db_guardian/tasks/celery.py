"""
Celery configuration for AuroraVault
异步任务队列配置
"""
import os
from celery import Celery
from celery.signals import setup_logging

# 设置默认的 Django settings 模块
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings')

# 创建 Celery 应用
app = Celery('auroravault')

# 使用 Django settings 配置 Celery
# namespace='CELERY' 表示所有 celery 配置项必须以 CELERY_ 开头
app.config_from_object('django.conf:settings', namespace='CELERY')

# 自动发现所有 Django apps 中的 tasks.py 文件
app.autodiscover_tasks()


@setup_logging.connect
def config_loggers(*args, **kwargs):
    """
    配置 Celery 日志使用 Django 的日志配置
    """
    from logging.config import dictConfig
    from django.conf import settings
    
    dictConfig(settings.LOGGING)


# 配置定时任务
app.conf.beat_schedule = {
    # 每60秒检查一次实例健康状态
    'check-instances-health': {
        'task': 'instances.check_instances_health',
        'schedule': 60.0,
    },
    # 每5分钟采集一次监控指标
    'collect-instances-metrics': {
        'task': 'instances.collect_instances_metrics',
        'schedule': 300.0,
    },
    # 每小时更新一次数据库统计信息
    'update-database-statistics': {
        'task': 'instances.update_database_statistics',
        'schedule': 3600.0,
    },
    # 每天凌晨2点清理30天前的监控数据
    'cleanup-old-metrics': {
        'task': 'instances.cleanup_old_metrics',
        'schedule': {
            'hour': 2,
            'minute': 0,
        },
        'kwargs': {'days': 30},
    },
}


@app.task(bind=True, ignore_result=True)
def debug_task(self):
    """
    调试任务,用于测试 Celery 是否正常工作
    """
    print(f'Request: {self.request!r}')
