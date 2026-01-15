"""
备份管理的 Celery 任务

包含自动备份、清理、验证等异步任务。
"""
from celery import shared_task
from django.utils import timezone
from datetime import timedelta
from pathlib import Path
import logging
import os

logger = logging.getLogger(__name__)


@shared_task(bind=True, max_retries=3)
def execute_backup_task(self, strategy_id=None, instance_id=None,
                        database_name=None, user_id=None, backup_type=None,
                        compress=None):
    """
    执行备份任务
    
    Args:
        self: Celery 任务实例
        strategy_id: 备份策略 ID（自动备份时提供）
        instance_id: MySQL 实例 ID
        database_name: 数据库名称（可选）
        user_id: 触发用户 ID（手动备份时提供）
        
    Returns:
        dict: 备份结果
    """
    from apps.backups.models import BackupStrategy, BackupRecord
    from apps.instances.models import MySQLInstance
    from apps.backups.services import BackupExecutor
    from apps.authentication.models import User
    
    backup_record = None
    
    try:
        # 1. 获取实例和策略
        if strategy_id:
            strategy = BackupStrategy.objects.select_related('instance').get(id=strategy_id)
            instance = strategy.instance
            databases = strategy.databases or [database_name] if database_name else None
            compress = strategy.compress
            storage_path = strategy.get_storage_path()
            backup_type = strategy.backup_type
        elif instance_id:
            instance = MySQLInstance.objects.get(id=instance_id)
            strategy = None
            databases = [database_name] if database_name else None
            compress = True if compress is None else compress
            storage_path = None
            backup_type = backup_type or 'full'
        else:
            raise ValueError("必须提供 strategy_id 或 instance_id")

        base_backup = None
        if backup_type == 'incremental':
            base_backup = BackupRecord.objects.filter(
                instance=instance,
                backup_type__in=['hot', 'incremental'],
                status='success'
            ).order_by('-created_at').first()
            if not base_backup:
                raise Exception("增量备份需要先有成功的热备/增量备份作为基准")
        
        # 2. 创建备份记录
        backup_record = BackupRecord.objects.create(
            instance=instance,
            strategy=strategy,
            database_name=database_name or '',
            backup_type=backup_type,
            status='running',
            start_time=timezone.now(),
            created_by_id=user_id,
            base_backup=base_backup,
            remote_path='',
            object_storage_path=''
        )
        
        logger.info(f"开始备份任务: 记录ID={backup_record.id}, 实例={instance.alias}")
        
        # 3. 执行备份
        executor = BackupExecutor(instance)
        
        # 如果有多个数据库，分别备份
        if databases and len(databases) > 0:
            for db in databases:
                result = executor.execute_backup(
                    database_name=db,
                    compress=compress,
                    storage_path=storage_path,
                    backup_type=backup_type,
                    base_backup=base_backup
                )
                
                if not result['success']:
                    raise Exception(f"数据库 {db} 备份失败: {result.get('error_message')}")
        else:
            # 备份所有数据库
            result = executor.execute_backup(
                database_name=database_name,
                compress=compress,
                storage_path=storage_path,
                backup_type=backup_type,
                base_backup=base_backup
            )
            
            if not result['success']:
                raise Exception(result.get('error_message'))
        
        # 4. 更新备份记录为成功
        backup_record.status = 'success'
        backup_record.end_time = timezone.now()
        backup_record.file_path = result['file_path']
        backup_record.file_size_mb = result['file_size_mb']
        backup_record.remote_path = result.get('remote_path', '')
        backup_record.object_storage_path = result.get('object_storage_path', '')
        backup_record.save()
        
        logger.info(f"备份任务完成: 记录ID={backup_record.id}")
        
        # 5. 触发清理任务（可选）
        if strategy and strategy.retention_days:
            cleanup_old_backups.delay(instance_id=instance.id, days=strategy.retention_days)
        
        return {
            'success': True,
            'backup_id': backup_record.id,
            'file_path': result['file_path'],
            'file_size_mb': result['file_size_mb'],
            'remote_path': result.get('remote_path', ''),
            'object_storage_path': result.get('object_storage_path', '')
        }
        
    except Exception as e:
        error_msg = str(e)
        logger.exception(f"备份任务失败: {error_msg}")
        
        # 更新备份记录为失败
        if backup_record:
            backup_record.status = 'failed'
            backup_record.end_time = timezone.now()
            backup_record.error_message = error_msg
            backup_record.save()
        
        # 重试任务
        if self.request.retries < self.max_retries:
            raise self.retry(exc=e, countdown=60 * (self.request.retries + 1))
        
        return {
            'success': False,
            'error_message': error_msg
        }


@shared_task
def cleanup_old_backups(instance_id=None, days=None):
    """
    清理过期备份文件
    
    Args:
        instance_id: MySQL 实例 ID（可选，不提供则清理所有实例）
        days: 保留天数（可选，不提供则使用策略配置）
        
    Returns:
        dict: 清理结果
    """
    from apps.backups.models import BackupRecord
    from apps.instances.models import MySQLInstance
    
    try:
        # 计算过期时间
        if days is None:
            days = 30  # 默认保留30天
        
        cutoff_time = timezone.now() - timedelta(days=days)
        
        # 构建查询
        query = BackupRecord.objects.filter(
            status='success',
            created_at__lt=cutoff_time
        )
        
        if instance_id:
            query = query.filter(instance_id=instance_id)
        
        # 获取过期记录
        expired_records = query.all()
        
        deleted_count = 0
        freed_space_mb = 0
        
        for record in expired_records:
            # 删除文件
            if record.file_path and os.path.exists(record.file_path):
                try:
                    file_path = Path(record.file_path)
                    file_size = file_path.stat().st_size / (1024 * 1024)
                    file_path.unlink()
                    freed_space_mb += file_size
                    logger.info(f"删除过期备份文件: {record.file_path}")
                except Exception as e:
                    logger.error(f"删除文件失败 {record.file_path}: {str(e)}")
            
            # 删除记录
            record.delete()
            deleted_count += 1
        
        logger.info(f"清理完成: 删除 {deleted_count} 个备份，释放 {freed_space_mb:.2f} MB")
        
        return {
            'success': True,
            'deleted_count': deleted_count,
            'freed_space_mb': round(freed_space_mb, 2)
        }
        
    except Exception as e:
        error_msg = str(e)
        logger.exception(f"清理任务失败: {error_msg}")
        return {
            'success': False,
            'error_message': error_msg
        }


@shared_task
def verify_backup_integrity(backup_id):
    """
    验证备份文件完整性
    
    Args:
        backup_id: 备份记录 ID
        
    Returns:
        dict: 验证结果
    """
    from apps.backups.models import BackupRecord
    import gzip
    
    try:
        # 获取备份记录
        backup_record = BackupRecord.objects.get(id=backup_id)
        
        if not backup_record.file_path:
            return {
                'success': False,
                'is_valid': False,
                'message': '备份文件路径为空'
            }
        
        file_path = Path(backup_record.file_path)
        
        # 1. 检查文件是否存在
        if not file_path.exists():
            return {
                'success': False,
                'is_valid': False,
                'message': '备份文件不存在'
            }
        
        # 2. 检查文件大小是否合理
        actual_size = file_path.stat().st_size / (1024 * 1024)
        if actual_size < 0.01:  # 小于10KB认为异常
            return {
                'success': False,
                'is_valid': False,
                'message': f'备份文件过小: {actual_size:.2f} MB'
            }
        
        # 3. 如果是压缩文件，尝试读取
        if file_path.suffix == '.gz':
            try:
                with gzip.open(file_path, 'rb') as f:
                    # 读取前1KB检查是否可以解压
                    f.read(1024)
            except Exception as e:
                return {
                    'success': False,
                    'is_valid': False,
                    'message': f'压缩文件损坏: {str(e)}'
                }
        
        # 4. 检查文件是否可读
        try:
            with open(file_path, 'rb') as f:
                # 读取前1KB检查是否可读
                f.read(1024)
        except Exception as e:
            return {
                'success': False,
                'is_valid': False,
                'message': f'文件不可读: {str(e)}'
            }
        
        logger.info(f"备份文件验证成功: {file_path}")
        
        return {
            'success': True,
            'is_valid': True,
            'message': '备份文件完整',
            'file_size_mb': round(actual_size, 2)
        }
        
    except BackupRecord.DoesNotExist:
        return {
            'success': False,
            'is_valid': False,
            'message': '备份记录不存在'
        }
    except Exception as e:
        error_msg = str(e)
        logger.exception(f"验证任务失败: {error_msg}")
        return {
            'success': False,
            'error_message': error_msg
        }


@shared_task
def check_backup_limits(instance_id):
    """
    检查实例的备份数量限制，删除超出的备份
    
    Args:
        instance_id: MySQL 实例 ID
        
    Returns:
        dict: 检查结果
    """
    from apps.backups.models import BackupRecord
    from django.conf import settings
    
    try:
        max_files = getattr(settings, 'BACKUP_MAX_FILES_PER_INSTANCE', 50)
        
        # 获取该实例的所有成功备份，按时间倒序
        backups = BackupRecord.objects.filter(
            instance_id=instance_id,
            status='success'
        ).order_by('-created_at')
        
        # 如果超出限制，删除最旧的备份
        if backups.count() > max_files:
            excess_backups = backups[max_files:]
            deleted_count = 0
            
            for backup in excess_backups:
                # 删除文件
                if backup.file_path and os.path.exists(backup.file_path):
                    try:
                        Path(backup.file_path).unlink()
                        logger.info(f"删除超限备份文件: {backup.file_path}")
                    except Exception as e:
                        logger.error(f"删除文件失败: {str(e)}")
                
                # 删除记录
                backup.delete()
                deleted_count += 1
            
            logger.info(f"实例 {instance_id} 清理超限备份: {deleted_count} 个")
            
            return {
                'success': True,
                'deleted_count': deleted_count,
                'current_count': max_files
            }
        
        return {
            'success': True,
            'deleted_count': 0,
            'current_count': backups.count()
        }
        
    except Exception as e:
        error_msg = str(e)
        logger.exception(f"检查备份限制失败: {error_msg}")
        return {
            'success': False,
            'error_message': error_msg
        }
