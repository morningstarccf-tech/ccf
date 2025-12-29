"""
备份管理服务类

包含备份执行、恢复执行、策略管理等核心功能。
"""
import os
import subprocess
import gzip
import shutil
from pathlib import Path
from datetime import datetime
from django.conf import settings
from django.utils import timezone
from django_celery_beat.models import PeriodicTask, CrontabSchedule
import logging
import json

logger = logging.getLogger(__name__)


class BackupExecutor:
    """
    备份执行器
    
    负责执行 MySQL 数据库备份操作。
    """
    
    def __init__(self, instance):
        """
        初始化备份执行器
        
        Args:
            instance: MySQLInstance 实例
        """
        self.instance = instance
    
    def execute_backup(self, database_name=None, compress=True, storage_path=None):
        """
        执行备份
        
        Args:
            database_name: 数据库名称，为 None 表示备份所有数据库
            compress: 是否压缩备份文件
            storage_path: 存储路径，为 None 则使用默认路径
            
        Returns:
            dict: 包含备份结果的字典
                - success: 是否成功
                - file_path: 备份文件路径
                - file_size_mb: 文件大小（MB）
                - error_message: 错误信息（如果失败）
        """
        try:
            # 1. 生成备份文件名
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            db_suffix = database_name if database_name else 'all'
            filename = f"{self.instance.alias}_{db_suffix}_{timestamp}.sql"
            
            # 2. 确定存储路径
            if storage_path is None:
                backup_root = getattr(settings, 'BACKUP_STORAGE_PATH', settings.BASE_DIR / 'backups')
                storage_path = Path(backup_root) / self.instance.alias
            else:
                storage_path = Path(storage_path)
            
            # 创建目录
            storage_path.mkdir(parents=True, exist_ok=True)
            
            # 完整文件路径
            file_path = storage_path / filename
            
            # 3. 构建 mysqldump 命令
            dump_cmd = self._build_mysqldump_command(database_name, str(file_path))
            
            # 4. 执行备份命令
            logger.info(f"开始备份: {self.instance.alias} - {db_suffix}")
            result = subprocess.run(
                dump_cmd,
                shell=True,
                capture_output=True,
                text=True,
                timeout=3600  # 1小时超时
            )
            
            if result.returncode != 0:
                error_msg = result.stderr or "备份命令执行失败"
                logger.error(f"备份失败: {error_msg}")
                return {
                    'success': False,
                    'error_message': error_msg
                }
            
            # 5. 压缩文件（如果需要）
            final_path = file_path
            if compress:
                compressed_path = self._compress_file(file_path)
                if compressed_path:
                    final_path = compressed_path
                    # 删除原始未压缩文件
                    if file_path.exists():
                        file_path.unlink()
            
            # 6. 计算文件大小
            file_size_mb = final_path.stat().st_size / (1024 * 1024)
            
            logger.info(f"备份成功: {final_path}, 大小: {file_size_mb:.2f} MB")
            
            return {
                'success': True,
                'file_path': str(final_path),
                'file_size_mb': round(file_size_mb, 2)
            }
            
        except subprocess.TimeoutExpired:
            error_msg = "备份超时（超过1小时）"
            logger.error(error_msg)
            return {
                'success': False,
                'error_message': error_msg
            }
        except Exception as e:
            error_msg = f"备份执行异常: {str(e)}"
            logger.exception(error_msg)
            return {
                'success': False,
                'error_message': error_msg
            }
    
    def _build_mysqldump_command(self, database_name, output_file):
        """
        构建 mysqldump 命令
        
        Args:
            database_name: 数据库名称，为 None 表示所有数据库
            output_file: 输出文件路径
            
        Returns:
            str: mysqldump 命令
        """
        # 获取解密后的密码
        password = self.instance.get_decrypted_password()
        
        # 基础命令
        cmd_parts = [
            'mysqldump',
            f'-h {self.instance.host}',
            f'-P {self.instance.port}',
            f'-u {self.instance.username}',
        ]
        
        # 添加密码（使用环境变量更安全，但为简化直接在命令中）
        if password:
            cmd_parts.append(f'-p"{password}"')
        
        # 添加常用选项
        cmd_parts.extend([
            '--single-transaction',  # 对于InnoDB，保证一致性备份
            '--quick',  # 快速导出，不缓冲到内存
            '--lock-tables=false',  # 不锁表
            '--set-gtid-purged=OFF',  # 不包含GTID信息
        ])
        
        # 指定数据库
        if database_name:
            cmd_parts.append(f'--databases {database_name}')
        else:
            cmd_parts.append('--all-databases')
        
        # 输出重定向
        cmd = ' '.join(cmd_parts) + f' > "{output_file}"'
        
        return cmd
    
    def _compress_file(self, file_path):
        """
        压缩备份文件
        
        Args:
            file_path: Path 对象，原始文件路径
            
        Returns:
            Path: 压缩后的文件路径，失败则返回 None
        """
        try:
            compressed_path = Path(str(file_path) + '.gz')
            
            with open(file_path, 'rb') as f_in:
                with gzip.open(compressed_path, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
            
            logger.info(f"文件压缩成功: {compressed_path}")
            return compressed_path
            
        except Exception as e:
            logger.error(f"文件压缩失败: {str(e)}")
            return None


class RestoreExecutor:
    """
    恢复执行器
    
    负责从备份文件恢复 MySQL 数据库。
    """
    
    def __init__(self, instance):
        """
        初始化恢复执行器
        
        Args:
            instance: MySQLInstance 实例
        """
        self.instance = instance
    
    def execute_restore(self, backup_file_path, target_database=None):
        """
        执行恢复
        
        Args:
            backup_file_path: 备份文件路径
            target_database: 目标数据库名称，为 None 则恢复到原数据库
            
        Returns:
            dict: 包含恢复结果的字典
                - success: 是否成功
                - error_message: 错误信息（如果失败）
        """
        try:
            file_path = Path(backup_file_path)
            
            # 1. 验证备份文件存在
            if not file_path.exists():
                return {
                    'success': False,
                    'error_message': f"备份文件不存在: {backup_file_path}"
                }
            
            # 2. 解压文件（如果需要）
            temp_file = None
            if file_path.suffix == '.gz':
                temp_file = self._decompress_file(file_path)
                if not temp_file:
                    return {
                        'success': False,
                        'error_message': "备份文件解压失败"
                    }
                restore_file = temp_file
            else:
                restore_file = file_path
            
            # 3. 构建 mysql 恢复命令
            restore_cmd = self._build_mysql_command(str(restore_file), target_database)
            
            # 4. 执行恢复
            logger.info(f"开始恢复: {self.instance.alias} - {backup_file_path}")
            result = subprocess.run(
                restore_cmd,
                shell=True,
                capture_output=True,
                text=True,
                timeout=3600  # 1小时超时
            )
            
            # 清理临时文件
            if temp_file and temp_file.exists():
                temp_file.unlink()
            
            if result.returncode != 0:
                error_msg = result.stderr or "恢复命令执行失败"
                logger.error(f"恢复失败: {error_msg}")
                return {
                    'success': False,
                    'error_message': error_msg
                }
            
            logger.info(f"恢复成功: {self.instance.alias}")
            return {'success': True}
            
        except subprocess.TimeoutExpired:
            error_msg = "恢复超时（超过1小时）"
            logger.error(error_msg)
            return {
                'success': False,
                'error_message': error_msg
            }
        except Exception as e:
            error_msg = f"恢复执行异常: {str(e)}"
            logger.exception(error_msg)
            return {
                'success': False,
                'error_message': error_msg
            }
    
    def _decompress_file(self, compressed_path):
        """
        解压备份文件
        
        Args:
            compressed_path: Path 对象，压缩文件路径
            
        Returns:
            Path: 解压后的临时文件路径，失败则返回 None
        """
        try:
            # 在同目录创建临时文件
            temp_path = compressed_path.parent / f"temp_{compressed_path.stem}"
            
            with gzip.open(compressed_path, 'rb') as f_in:
                with open(temp_path, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
            
            logger.info(f"文件解压成功: {temp_path}")
            return temp_path
            
        except Exception as e:
            logger.error(f"文件解压失败: {str(e)}")
            return None
    
    def _build_mysql_command(self, input_file, target_database=None):
        """
        构建 mysql 恢复命令
        
        Args:
            input_file: 输入文件路径
            target_database: 目标数据库名称
            
        Returns:
            str: mysql 命令
        """
        # 获取解密后的密码
        password = self.instance.get_decrypted_password()
        
        # 基础命令
        cmd_parts = [
            'mysql',
            f'-h {self.instance.host}',
            f'-P {self.instance.port}',
            f'-u {self.instance.username}',
        ]
        
        # 添加密码
        if password:
            cmd_parts.append(f'-p"{password}"')
        
        # 指定目标数据库（如果提供）
        if target_database:
            cmd_parts.append(target_database)
        
        # 输入重定向
        cmd = ' '.join(cmd_parts) + f' < "{input_file}"'
        
        return cmd


class StrategyManager:
    """
    策略管理器
    
    负责同步备份策略到 Celery Beat。
    """
    
    @staticmethod
    def sync_to_celery_beat():
        """
        同步所有启用的备份策略到 Celery Beat
        
        Returns:
            dict: 同步结果统计
                - created: 创建的任务数
                - updated: 更新的任务数
                - deleted: 删除的任务数
        """
        from apps.backups.models import BackupStrategy
        
        created_count = 0
        updated_count = 0
        deleted_count = 0
        
        try:
            # 1. 获取所有启用的策略
            enabled_strategies = BackupStrategy.objects.filter(is_enabled=True)
            
            # 2. 为每个策略创建或更新 PeriodicTask
            for strategy in enabled_strategies:
                task_created = StrategyManager._create_or_update_periodic_task(strategy)
                if task_created:
                    created_count += 1
                else:
                    updated_count += 1
            
            # 3. 删除已禁用策略的任务
            disabled_strategies = BackupStrategy.objects.filter(is_enabled=False)
            for strategy in disabled_strategies:
                if StrategyManager._delete_periodic_task(strategy):
                    deleted_count += 1
            
            logger.info(f"策略同步完成: 创建 {created_count}, 更新 {updated_count}, 删除 {deleted_count}")
            
            return {
                'created': created_count,
                'updated': updated_count,
                'deleted': deleted_count
            }
            
        except Exception as e:
            logger.exception(f"策略同步失败: {str(e)}")
            raise
    
    @staticmethod
    def _create_or_update_periodic_task(strategy):
        """
        为策略创建或更新 PeriodicTask
        
        Args:
            strategy: BackupStrategy 实例
            
        Returns:
            bool: True 表示创建了新任务，False 表示更新了现有任务
        """
        # 解析 Cron 表达式
        cron_schedule = StrategyManager._parse_cron_expression(strategy.cron_expression)
        
        # 任务名称
        task_name = f"backup_strategy_{strategy.id}"
        
        # 任务参数
        task_kwargs = {
            'strategy_id': strategy.id,
        }
        
        # 创建或更新任务
        task, created = PeriodicTask.objects.update_or_create(
            name=task_name,
            defaults={
                'crontab': cron_schedule,
                'task': 'apps.backups.tasks.execute_backup_task',
                'kwargs': json.dumps(task_kwargs),
                'enabled': True,
            }
        )
        
        return created
    
    @staticmethod
    def _delete_periodic_task(strategy):
        """
        删除策略对应的 PeriodicTask
        
        Args:
            strategy: BackupStrategy 实例
            
        Returns:
            bool: 是否删除了任务
        """
        task_name = f"backup_strategy_{strategy.id}"
        deleted, _ = PeriodicTask.objects.filter(name=task_name).delete()
        return deleted > 0
    
    @staticmethod
    def _parse_cron_expression(cron_expr):
        """
        解析 Cron 表达式并创建 CrontabSchedule
        
        Args:
            cron_expr: Cron 表达式字符串，如 "0 2 * * *"
            
        Returns:
            CrontabSchedule: Crontab 调度对象
        """
        # 解析 Cron 表达式（格式：分 时 日 月 周）
        parts = cron_expr.strip().split()
        if len(parts) != 5:
            raise ValueError(f"无效的 Cron 表达式: {cron_expr}")
        
        minute, hour, day_of_month, month_of_year, day_of_week = parts
        
        # 创建或获取 CrontabSchedule
        schedule, _ = CrontabSchedule.objects.get_or_create(
            minute=minute,
            hour=hour,
            day_of_month=day_of_month,
            month_of_year=month_of_year,
            day_of_week=day_of_week,
        )
        
        return schedule