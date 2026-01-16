"""
备份管理服务类

包含备份执行、恢复执行、策略管理等核心功能。
"""
import os
import shlex
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
import paramiko

logger = logging.getLogger(__name__)

try:
    import oss2
except ImportError:  # pragma: no cover - optional dependency
    oss2 = None


class RemoteExecutor:
    """远程命令执行与文件传输（支持本地直连）"""

    def __init__(self, instance):
        self.instance = instance
        self.host = instance.ssh_host.strip() if instance.ssh_host else ''
        self.port = instance.ssh_port or 22
        self.user = instance.ssh_user.strip() if instance.ssh_user else ''
        self.password = instance.get_decrypted_ssh_password() if instance.ssh_password else None
        self.key_path = instance.ssh_key_path.strip() if instance.ssh_key_path else ''

    def _is_remote(self) -> bool:
        return bool(self.host and self.user)

    def _connect(self):
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        if self.key_path:
            key = paramiko.RSAKey.from_private_key_file(self.key_path)
            client.connect(self.host, port=self.port, username=self.user, pkey=key, timeout=10)
        else:
            client.connect(self.host, port=self.port, username=self.user, password=self.password, timeout=10)
        return client

    def run(self, command: str, timeout: int = 3600) -> tuple[int, str, str]:
        if not self._is_remote():
            result = subprocess.run(
                command,
                shell=True,
                capture_output=True,
                text=True,
                timeout=timeout
            )
            return result.returncode, result.stdout, result.stderr

        client = self._connect()
        try:
            stdin, stdout, stderr = client.exec_command(command, timeout=timeout)
            exit_status = stdout.channel.recv_exit_status()
            return exit_status, stdout.read().decode(), stderr.read().decode()
        finally:
            client.close()

    def download(self, remote_path: str, local_path: Path) -> None:
        if not self._is_remote():
            shutil.copy2(remote_path, local_path)
            return

        client = self._connect()
        try:
            sftp = client.open_sftp()
            sftp.get(remote_path, str(local_path))
            sftp.close()
        finally:
            client.close()

    def upload(self, local_path: Path, remote_path: str) -> None:
        if not self._is_remote():
            shutil.copy2(local_path, remote_path)
            return

        client = self._connect()
        try:
            sftp = client.open_sftp()
            sftp.put(str(local_path), remote_path)
            sftp.close()
        finally:
            client.close()


class ObjectStorageUploader:
    """对象存储上传（Aliyun OSS）。"""

    def __init__(self):
        self.enabled = getattr(settings, 'OSS_ENABLED', False)
        self.endpoint = getattr(settings, 'OSS_ENDPOINT', '')
        self.access_key_id = getattr(settings, 'OSS_ACCESS_KEY_ID', '')
        self.access_key_secret = getattr(settings, 'OSS_ACCESS_KEY_SECRET', '')
        self.bucket = getattr(settings, 'OSS_BUCKET', '')
        self.prefix = getattr(settings, 'OSS_PREFIX', '')

    def _is_ready(self) -> bool:
        return bool(
            self.enabled and oss2 and self.endpoint and self.access_key_id and
            self.access_key_secret and self.bucket
        )

    def upload(self, local_path: Path, instance_alias: str, filename: str) -> str | None:
        if not self._is_ready():
            return None

        prefix = str(self.prefix).strip('/')
        parts = [p for p in [prefix, instance_alias, filename] if p]
        object_key = '/'.join(parts)

        auth = oss2.Auth(self.access_key_id, self.access_key_secret)
        bucket = oss2.Bucket(auth, self.endpoint, self.bucket)
        result = bucket.put_object_from_file(object_key, str(local_path))
        if result.status not in (200, 201):
            raise RuntimeError(f'OSS 上传失败: status={result.status}')
        return f"oss://{self.bucket}/{object_key}"


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

    def _get_remote_backup_path(self, filename: str, executor: RemoteExecutor) -> str | None:
        remote_root = (self.instance.remote_backup_root or '').strip()
        if not remote_root:
            return None
        if not executor._is_remote():
            logger.warning("远程备份目录已配置，但未设置 SSH 连接信息")
            return None
        safe_alias = self.instance.alias.replace(' ', '_')
        remote_dir = f"{remote_root.rstrip('/')}/{safe_alias}"
        executor.run(f"mkdir -p {shlex.quote(remote_dir)}")
        return f"{remote_dir}/{filename}"

    def _upload_to_remote(self, local_path: Path, filename: str) -> str | None:
        executor = RemoteExecutor(self.instance)
        remote_path = self._get_remote_backup_path(filename, executor)
        if not remote_path:
            return None
        executor.upload(local_path, remote_path)
        return remote_path

    def _upload_to_object_storage(self, local_path: Path, filename: str) -> str | None:
        uploader = ObjectStorageUploader()
        try:
            return uploader.upload(local_path, self.instance.alias, filename)
        except Exception as exc:
            logger.warning(f"OSS 上传失败: {exc}")
            return None
    
    def execute_backup(self, database_name=None, compress=True, storage_path=None,
                       backup_type='full', base_backup=None):
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
            if backup_type in ['hot', 'cold', 'incremental'] and database_name:
                return {
                    'success': False,
                    'error_message': '热备/冷备/增量备份不支持指定单个数据库'
                }

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
            
            if backup_type in ['full']:
                return self._execute_logical_backup(
                    database_name, compress, storage_path, filename
                )

            if backup_type in ['hot']:
                return self._execute_hot_backup(storage_path, timestamp, compress)

            if backup_type in ['cold']:
                return self._execute_cold_backup(storage_path, timestamp, compress)

            if backup_type in ['incremental']:
                return self._execute_incremental_backup(storage_path, timestamp, compress, base_backup)

            return {
                'success': False,
                'error_message': f'不支持的备份类型: {backup_type}'
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
    
    def _get_dump_binary(self) -> str | None:
        """获取可用的导出命令（mysqldump 或 mariadb-dump）。"""
        return shutil.which('mysqldump') or shutil.which('mariadb-dump')

    def _build_mysqldump_command(self, database_name, output_file, dump_bin: str):
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
            dump_bin,
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
        ])
        
        # 指定数据库
        if database_name:
            cmd_parts.append(f'--databases {database_name}')
        else:
            cmd_parts.append('--all-databases')
        
        # 输出重定向
        cmd = ' '.join(cmd_parts) + f' > "{output_file}"'
        
        return cmd

    def _execute_logical_backup(self, database_name, compress, storage_path, filename):
        """执行逻辑备份（mysqldump）"""
        dump_bin = self._get_dump_binary()
        if not dump_bin:
            return {
                'success': False,
                'error_message': 'mysqldump 或 mariadb-dump 未安装'
            }

        file_path = storage_path / filename
        dump_cmd = self._build_mysqldump_command(database_name, str(file_path), dump_bin)

        logger.info(f"开始逻辑备份: {self.instance.alias}")
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

        final_path = file_path
        if compress:
            compressed_path = self._compress_file(file_path)
            if compressed_path:
                final_path = compressed_path
                if file_path.exists():
                    file_path.unlink()

        file_size_mb = final_path.stat().st_size / (1024 * 1024)
        logger.info(f"备份成功: {final_path}, 大小: {file_size_mb:.2f} MB")

        remote_path = None
        if self.instance.remote_backup_root:
            try:
                remote_path = self._upload_to_remote(final_path, final_path.name)
            except Exception as exc:
                logger.warning(f"远程备份上传失败: {exc}")

        object_storage_path = self._upload_to_object_storage(final_path, final_path.name)

        return {
            'success': True,
            'file_path': str(final_path),
            'file_size_mb': round(file_size_mb, 2),
            'remote_path': remote_path or '',
            'object_storage_path': object_storage_path or ''
        }

    def _build_xtrabackup_command(self, target_dir, incremental_base_dir=None):
        """构建 xtrabackup 命令"""
        password = self.instance.get_decrypted_password()
        cmd_parts = [
            shlex.quote(self.instance.xtrabackup_bin or 'xtrabackup'),
            '--backup',
            f'--target-dir={shlex.quote(target_dir)}',
            f'--datadir={shlex.quote(self.instance.data_dir)}',
            f'--host={shlex.quote(self.instance.host)}',
            f'--port={self.instance.port}',
            f'--user={shlex.quote(self.instance.username)}',
        ]
        if password:
            cmd_parts.append(f'--password={shlex.quote(password)}')
        if incremental_base_dir:
            cmd_parts.append(f'--incremental-basedir={shlex.quote(incremental_base_dir)}')
        return ' '.join(cmd_parts)

    def _remote_root(self):
        safe_alias = self.instance.alias.replace(' ', '_')
        return f"/tmp/db_guardian/{safe_alias}"

    def _archive_remote_dir(self, executor, remote_dir, compress):
        parent_dir = str(Path(remote_dir).parent)
        base_name = Path(remote_dir).name
        suffix = '.tar.gz' if compress else '.tar'
        archive_path = f"{remote_dir}{suffix}"
        tar_flag = '-czf' if compress else '-cf'
        cmd = (
            f"tar -C {shlex.quote(parent_dir)} {tar_flag} "
            f"{shlex.quote(archive_path)} {shlex.quote(base_name)}"
        )
        code, _, err = executor.run(cmd, timeout=3600)
        if code != 0:
            raise RuntimeError(err or "打包备份目录失败")
        return archive_path

    def _strip_archive_suffix(self, filename: str) -> str:
        name = Path(filename).name
        if name.endswith('.tar.gz'):
            return name[:-7]
        if name.endswith('.tar'):
            return name[:-4]
        return Path(name).stem

    def _execute_hot_backup(self, storage_path, timestamp, compress):
        """执行热备（xtrabackup 全量）"""
        if not self.instance.data_dir:
            return {'success': False, 'error_message': '未配置实例数据目录'}

        executor = RemoteExecutor(self.instance)
        remote_root = self._remote_root()
        backup_dir_name = f"hot_{self.instance.alias}_{timestamp}".replace(' ', '_')
        remote_dir = f"{remote_root}/{backup_dir_name}"
        archive_name = f"{backup_dir_name}.tar.gz" if compress else f"{backup_dir_name}.tar"
        local_path = Path(storage_path) / archive_name

        executor.run(f"mkdir -p {shlex.quote(remote_root)}")
        executor.run(f"mkdir -p {shlex.quote(remote_dir)}")

        cmd = self._build_xtrabackup_command(remote_dir)
        code, _, err = executor.run(cmd, timeout=3600)
        if code != 0:
            return {'success': False, 'error_message': err or '热备失败'}

        remote_archive = self._archive_remote_dir(executor, remote_dir, compress)
        remote_keep_path = self._get_remote_backup_path(archive_name, executor)
        download_source = remote_archive

        if remote_keep_path:
            move_cmd = f"mv {shlex.quote(remote_archive)} {shlex.quote(remote_keep_path)}"
            code, _, err = executor.run(move_cmd, timeout=600)
            if code != 0:
                return {'success': False, 'error_message': err or '远程备份保存失败'}
            download_source = remote_keep_path

        executor.download(download_source, local_path)
        executor.run(f"rm -rf {shlex.quote(remote_dir)}")
        if not remote_keep_path:
            executor.run(f"rm -f {shlex.quote(remote_archive)}")

        file_size_mb = local_path.stat().st_size / (1024 * 1024)
        object_storage_path = self._upload_to_object_storage(local_path, local_path.name)
        return {
            'success': True,
            'file_path': str(local_path),
            'file_size_mb': round(file_size_mb, 2),
            'remote_path': remote_keep_path or '',
            'object_storage_path': object_storage_path or ''
        }

    def _execute_incremental_backup(self, storage_path, timestamp, compress, base_backup):
        """执行增量备份（xtrabackup 增量）"""
        if not base_backup or not base_backup.file_path:
            return {'success': False, 'error_message': '增量备份缺少基准备份'}
        if not self.instance.data_dir:
            return {'success': False, 'error_message': '未配置实例数据目录'}

        executor = RemoteExecutor(self.instance)
        remote_root = self._remote_root()
        backup_dir_name = f"incremental_{self.instance.alias}_{timestamp}".replace(' ', '_')
        remote_dir = f"{remote_root}/{backup_dir_name}"
        archive_name = f"{backup_dir_name}.tar.gz" if compress else f"{backup_dir_name}.tar"
        local_path = Path(storage_path) / archive_name

        base_archive = Path(base_backup.file_path)
        base_name = self._strip_archive_suffix(base_archive.name)
        remote_base_archive = f"{remote_root}/{base_archive.name}"
        remote_base_dir = f"{remote_root}/{base_name}"

        executor.run(f"mkdir -p {shlex.quote(remote_root)}")
        executor.upload(base_archive, remote_base_archive)
        extract_flag = '-xzf' if base_archive.name.endswith('.tar.gz') else '-xf'
        executor.run(
            f"tar -C {shlex.quote(remote_root)} {extract_flag} "
            f"{shlex.quote(remote_base_archive)}"
        )
        executor.run(f"mkdir -p {shlex.quote(remote_dir)}")

        cmd = self._build_xtrabackup_command(remote_dir, incremental_base_dir=remote_base_dir)
        code, _, err = executor.run(cmd, timeout=3600)
        if code != 0:
            return {'success': False, 'error_message': err or '增量备份失败'}

        remote_archive = self._archive_remote_dir(executor, remote_dir, compress)
        remote_keep_path = self._get_remote_backup_path(archive_name, executor)
        download_source = remote_archive

        if remote_keep_path:
            move_cmd = f"mv {shlex.quote(remote_archive)} {shlex.quote(remote_keep_path)}"
            code, _, err = executor.run(move_cmd, timeout=600)
            if code != 0:
                return {'success': False, 'error_message': err or '远程备份保存失败'}
            download_source = remote_keep_path

        executor.download(download_source, local_path)
        executor.run(
            f"rm -rf {shlex.quote(remote_dir)} "
            f"{shlex.quote(remote_base_dir)} {shlex.quote(remote_base_archive)}"
        )
        if not remote_keep_path:
            executor.run(f"rm -f {shlex.quote(remote_archive)}")

        file_size_mb = local_path.stat().st_size / (1024 * 1024)
        object_storage_path = self._upload_to_object_storage(local_path, local_path.name)
        return {
            'success': True,
            'file_path': str(local_path),
            'file_size_mb': round(file_size_mb, 2),
            'remote_path': remote_keep_path or '',
            'object_storage_path': object_storage_path or ''
        }

    def _execute_cold_backup(self, storage_path, timestamp, compress):
        """执行冷备（停库复制数据目录）"""
        if not self.instance.data_dir:
            return {'success': False, 'error_message': '未配置实例数据目录'}

        executor = RemoteExecutor(self.instance)
        remote_root = self._remote_root()
        backup_dir_name = f"cold_{self.instance.alias}_{timestamp}".replace(' ', '_')
        archive_name = f"{backup_dir_name}.tar.gz" if compress else f"{backup_dir_name}.tar"
        remote_archive = f"{remote_root}/{archive_name}"
        local_path = Path(storage_path) / archive_name

        if self.instance.deployment_type == 'docker':
            stop_cmd = f"docker stop {shlex.quote(self.instance.docker_container_name)}"
            start_cmd = f"docker start {shlex.quote(self.instance.docker_container_name)}"
        else:
            stop_cmd = f"sudo systemctl stop {shlex.quote(self.instance.mysql_service_name)}"
            start_cmd = f"sudo systemctl start {shlex.quote(self.instance.mysql_service_name)}"

        data_dir = Path(self.instance.data_dir)
        parent_dir = str(data_dir.parent)
        base_name = data_dir.name
        tar_flag = '-czf' if compress else '-cf'
        tar_cmd = (
            f"tar -C {shlex.quote(parent_dir)} {tar_flag} "
            f"{shlex.quote(remote_archive)} {shlex.quote(base_name)}"
        )

        executor.run(f"mkdir -p {shlex.quote(remote_root)}")
        try:
            code, _, err = executor.run(stop_cmd, timeout=600)
            if code != 0:
                return {'success': False, 'error_message': err or '停止 MySQL 失败'}

            code, _, err = executor.run(tar_cmd, timeout=3600)
            if code != 0:
                return {'success': False, 'error_message': err or '冷备份打包失败'}
        finally:
            executor.run(start_cmd, timeout=600)

        remote_keep_path = self._get_remote_backup_path(archive_name, executor)
        download_source = remote_archive
        if remote_keep_path:
            move_cmd = f"mv {shlex.quote(remote_archive)} {shlex.quote(remote_keep_path)}"
            code, _, err = executor.run(move_cmd, timeout=600)
            if code != 0:
                return {'success': False, 'error_message': err or '远程备份保存失败'}
            download_source = remote_keep_path

        executor.download(download_source, local_path)
        if not remote_keep_path:
            executor.run(f"rm -f {shlex.quote(remote_archive)}")

        file_size_mb = local_path.stat().st_size / (1024 * 1024)
        object_storage_path = self._upload_to_object_storage(local_path, local_path.name)
        return {
            'success': True,
            'file_path': str(local_path),
            'file_size_mb': round(file_size_mb, 2),
            'remote_path': remote_keep_path or '',
            'object_storage_path': object_storage_path or ''
        }
    
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
