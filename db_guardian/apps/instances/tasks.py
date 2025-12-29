"""
MySQL 实例管理的 Celery 定时任务

提供定期健康检查和监控指标采集等后台任务。
"""
from celery import shared_task
from django.utils import timezone
from apps.instances.models import MySQLInstance
from apps.instances.services import HealthChecker, MetricsCollector
import logging

logger = logging.getLogger(__name__)


@shared_task(name='instances.check_instances_health')
def check_instances_health():
    """
    定期检查所有实例的健康状态
    
    执行频率：每 60 秒执行一次
    
    功能：
    - 遍历所有实例
    - 执行健康检查
    - 更新实例状态和版本信息
    - 记录最后检查时间
    """
    logger.info("Starting health check for all instances")
    
    instances = MySQLInstance.objects.all()
    total_count = instances.count()
    online_count = 0
    offline_count = 0
    error_count = 0
    
    for instance in instances:
        try:
            # 执行健康检查
            is_healthy, message, info = HealthChecker.check_instance(instance)
            
            # 更新状态
            if is_healthy:
                instance.status = 'online'
                online_count += 1
                
                # 更新版本信息
                if 'version' in info and not instance.version:
                    instance.version = info['version']
            else:
                # 根据错误类型判断是离线还是错误
                if 'timeout' in message.lower() or 'connection refused' in message.lower():
                    instance.status = 'offline'
                    offline_count += 1
                else:
                    instance.status = 'error'
                    error_count += 1
            
            instance.last_check_time = timezone.now()
            instance.save(update_fields=['status', 'last_check_time', 'version'])
            
            logger.debug(f"Health check completed for {instance.alias}: {instance.status}")
        
        except Exception as e:
            logger.error(f"Health check failed for {instance.alias}: {str(e)}")
            instance.status = 'error'
            instance.last_check_time = timezone.now()
            instance.save(update_fields=['status', 'last_check_time'])
            error_count += 1
    
    logger.info(
        f"Health check completed: total={total_count}, "
        f"online={online_count}, offline={offline_count}, error={error_count}"
    )
    
    return {
        'total': total_count,
        'online': online_count,
        'offline': offline_count,
        'error': error_count
    }


@shared_task(name='instances.collect_instances_metrics')
def collect_instances_metrics():
    """
    定期采集所有实例的监控指标
    
    执行频率：每 5 分钟执行一次
    
    功能：
    - 遍历所有在线实例
    - 采集性能监控数据
    - 保存到监控指标表
    """
    logger.info("Starting metrics collection for all instances")
    
    # 只采集在线实例的指标
    instances = MySQLInstance.objects.filter(status='online')
    total_count = instances.count()
    success_count = 0
    failed_count = 0
    
    for instance in instances:
        try:
            # 采集指标
            metrics = MetricsCollector.collect_metrics(instance)
            
            if metrics:
                # 保存指标
                success = MetricsCollector.save_metrics(instance, metrics)
                
                if success:
                    success_count += 1
                    logger.debug(f"Metrics collected for {instance.alias}")
                else:
                    failed_count += 1
                    logger.warning(f"Failed to save metrics for {instance.alias}")
            else:
                failed_count += 1
                logger.warning(f"Failed to collect metrics for {instance.alias}")
        
        except Exception as e:
            logger.error(f"Metrics collection failed for {instance.alias}: {str(e)}")
            failed_count += 1
    
    logger.info(
        f"Metrics collection completed: total={total_count}, "
        f"success={success_count}, failed={failed_count}"
    )
    
    return {
        'total': total_count,
        'success': success_count,
        'failed': failed_count
    }


@shared_task(name='instances.cleanup_old_metrics')
def cleanup_old_metrics(days=30):
    """
    清理过期的监控指标数据
    
    执行频率：每天凌晨 2 点执行一次
    
    参数：
        days: 保留最近多少天的数据（默认 30 天）
    
    功能：
    - 删除超过指定天数的监控指标
    - 释放数据库存储空间
    """
    from apps.instances.models import MonitoringMetrics
    
    logger.info(f"Starting cleanup of metrics older than {days} days")
    
    cutoff_date = timezone.now() - timezone.timedelta(days=days)
    
    # 删除过期数据
    deleted_count, _ = MonitoringMetrics.objects.filter(
        timestamp__lt=cutoff_date
    ).delete()
    
    logger.info(f"Cleanup completed: deleted {deleted_count} old metrics records")
    
    return {
        'deleted': deleted_count,
        'cutoff_date': cutoff_date.isoformat()
    }


@shared_task(name='instances.update_database_statistics')
def update_database_statistics():
    """
    更新所有数据库的统计信息
    
    执行频率：每小时执行一次
    
    功能：
    - 遍历所有在线实例的数据库
    - 更新数据库大小和表数量
    """
    from apps.instances.models import Database
    
    logger.info("Starting database statistics update")
    
    # 获取所有在线实例的数据库
    databases = Database.objects.filter(
        instance__status='online'
    ).select_related('instance')
    
    total_count = databases.count()
    success_count = 0
    failed_count = 0
    
    for database in databases:
        try:
            database.update_statistics()
            success_count += 1
            logger.debug(f"Updated statistics for {database}")
        except Exception as e:
            logger.error(f"Failed to update statistics for {database}: {str(e)}")
            failed_count += 1
    
    logger.info(
        f"Database statistics update completed: total={total_count}, "
        f"success={success_count}, failed={failed_count}"
    )
    
    return {
        'total': total_count,
        'success': success_count,
        'failed': failed_count
    }