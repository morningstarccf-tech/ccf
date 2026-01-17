"""
备份管理服务类

包含备份执行、恢复执行、策略管理等核心功能。
"""
import os
import shlex
import subprocess
import gzip
import shutil
import ftplib
import requests
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

    def __init__(
        self,
        instance=None,
        host=None,
        port=None,
        user=None,
        password=None,
        key_path=None
    ):
        self.instance = instance
        if instance:
            self.host = (host or instance.ssh_host or '').strip()
            self.port = port or instance.ssh_port or 22
            self.user = (user or instance.ssh_user or '').strip()
            if password is None and instance.ssh_password:
                password = instance.get_decrypted_ssh_password()
            self.password = password
            self.key_path = (key_path or instance.ssh_key_path or '').strip()
        else:
            self.host = (host or '').strip()
            self.port = port or 22
            self.user = (user or '').strip()
            self.password = password
            self.key_path = (key_path or '').strip()

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


class RemoteStorageClient:
    """远程存储客户端（SSH/FTP/HTTP）"""

    def __init__(self, protocol: str, host: str, port: int | None, user: str | None,
                 password: str | None, key_path: str | None):
        self.protocol = (protocol or '').lower()
        self.host = (host or '').strip()
        self.port = port
        self.user = user or ''
        self.password = password
        self.key_path = key_path or ''

    def _ensure_ready(self):
        if not self.protocol:
            raise ValueError('远程协议未设置')
        if not self.host:
            raise ValueError('远程主机未设置')

    def _build_http_url(self, remote_path: str) -> str:
        if remote_path.startswith('http://') or remote_path.startswith('https://'):
            return remote_path
        port = self.port or 80
        base = f"{self.protocol}://{self.host}:{port}"
        return f"{base}/{remote_path.lstrip('/')}"

    def _ftp_connect(self) -> ftplib.FTP:
        port = self.port or 21
        ftp = ftplib.FTP()
        ftp.connect(self.host, port, timeout=10)
        ftp.login(self.user, self.password or '')
        return ftp

    def _ftp_ensure_dir(self, ftp: ftplib.FTP, dir_path: str) -> None:
        if not dir_path:
            return
        parts = [p for p in dir_path.strip('/').split('/') if p]
        current = ''
        for part in parts:
            current = f"{current}/{part}" if current else part
            try:
                ftp.mkd(current)
            except Exception:
                pass

    def test(self) -> tuple[bool, str]:
        self._ensure_ready()
        if self.protocol == 'ssh':
            executor = RemoteExecutor(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                key_path=self.key_path
            )
            code, _, stderr = executor.run('echo ok', timeout=10)
            return (code == 0, stderr or 'ok')
        if self.protocol == 'ftp':
            try:
                ftp = self._ftp_connect()
                ftp.quit()
                return True, 'ok'
            except Exception as exc:
                return False, str(exc)
        if self.protocol == 'http':
            try:
                url = self._build_http_url('/')
                auth = (self.user, self.password) if self.user or self.password else None
                resp = requests.head(url, auth=auth, timeout=10)
                if resp.status_code >= 400:
                    return False, f"HTTP 状态码 {resp.status_code}"
                return True, 'ok'
            except Exception as exc:
                return False, str(exc)
        return False, f"不支持的协议: {self.protocol}"

    def upload(self, local_path: Path, remote_path: str) -> str:
        self._ensure_ready()
        if self.protocol == 'ssh':
            executor = RemoteExecutor(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                key_path=self.key_path
            )
            remote_dir = str(Path(remote_path).parent).replace('\\', '/')
            if remote_dir:
                executor.run(f"mkdir -p {shlex.quote(remote_dir)}")
            executor.upload(local_path, remote_path)
            return remote_path
        if self.protocol == 'ftp':
            ftp = self._ftp_connect()
            try:
                remote_dir = str(Path(remote_path).parent).replace('\\', '/')
                self._ftp_ensure_dir(ftp, remote_dir)
                if remote_dir:
                    ftp.cwd(remote_dir)
                with open(local_path, 'rb') as f_in:
                    ftp.storbinary(f"STOR {Path(remote_path).name}", f_in)
                return remote_path
            finally:
                try:
                    ftp.quit()
                except Exception:
                    ftp.close()
        if self.protocol == 'http':
            url = self._build_http_url(remote_path)
            auth = (self.user, self.password) if self.user or self.password else None
            with open(local_path, 'rb') as f_in:
                resp = requests.put(url, data=f_in, auth=auth, timeout=30)
            if resp.status_code >= 400:
                raise RuntimeError(f"HTTP 上传失败: {resp.status_code} {resp.text}")
            return url
        raise RuntimeError(f"不支持的协议: {self.protocol}")

    def download(self, remote_path: str, local_path: Path) -> None:
        self._ensure_ready()
        if self.protocol == 'ssh':
            executor = RemoteExecutor(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                key_path=self.key_path
            )
            executor.download(remote_path, local_path)
            return
        if self.protocol == 'ftp':
            ftp = self._ftp_connect()
            try:
                remote_dir = str(Path(remote_path).parent).replace('\\', '/')
                if remote_dir:
                    ftp.cwd(remote_dir)
                with open(local_path, 'wb') as f_out:
                    ftp.retrbinary(f"RETR {Path(remote_path).name}", f_out.write)
            finally:
                try:
                    ftp.quit()
                except Exception:
                    ftp.close()
            return
        if self.protocol == 'http':
            url = self._build_http_url(remote_path)
            auth = (self.user, self.password) if self.user or self.password else None
            resp = requests.get(url, auth=auth, stream=True, timeout=30)
            if resp.status_code >= 400:
                raise RuntimeError(f"HTTP 下载失败: {resp.status_code} {resp.text}")
            with open(local_path, 'wb') as f_out:
                for chunk in resp.iter_content(chunk_size=8192):
                    if chunk:
                        f_out.write(chunk)
            return
        raise RuntimeError(f"不支持的协议: {self.protocol}")


class ObjectStorageUploader:
    """对象存储上传（Aliyun OSS）。"""

    def __init__(self, config: dict | None = None):
        if config:
            self.enabled = True
            self.endpoint = config.get('endpoint', '') or ''
            self.access_key_id = config.get('access_key_id', '') or ''
            self.access_key_secret = config.get('access_key_secret', '') or ''
            self.bucket = config.get('bucket', '') or ''
            self.prefix = config.get('prefix', '') or ''
        else:
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

    def _parse_object_path(self, object_path: str) -> tuple[str, str]:
        path = object_path.strip()
        if path.startswith('oss://'):
            stripped = path[len('oss://'):]
            bucket, _, key = stripped.partition('/')
            if not bucket or not key:
                raise ValueError('无效的 OSS 路径')
            return bucket, key
        return self.bucket, path.lstrip('/')

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

    def download(self, object_path: str, local_path: Path) -> None:
        if not self._is_ready():
            raise RuntimeError('OSS 未配置或不可用')
        bucket_name, object_key = self._parse_object_path(object_path)
        auth = oss2.Auth(self.access_key_id, self.access_key_secret)
        bucket = oss2.Bucket(auth, self.endpoint, bucket_name)
        result = bucket.get_object_to_file(object_key, str(local_path))
        if result.status not in (200, 201, 206):
            raise RuntimeError(f'OSS 下载失败: status={result.status}')

    def test_connection(self) -> tuple[bool, str]:
        if not self._is_ready():
            return False, 'OSS 未配置或不可用'
        try:
            auth = oss2.Auth(self.access_key_id, self.access_key_secret)
            bucket = oss2.Bucket(auth, self.endpoint, self.bucket)
            bucket.get_bucket_info()
            return True, 'ok'
        except Exception as exc:
            return False, str(exc)


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

    def _get_remote_backup_path(
        self,
        filename: str,
        executor: RemoteExecutor,
        remote_root_override: str | None = None
    ) -> str | None:
        remote_root = (remote_root_override or self.instance.remote_backup_root or '').strip()
        if not remote_root:
            return None
        if not executor._is_remote():
            logger.warning("远程备份目录已配置，但未设置 SSH 连接信息")
            return None
        if remote_root_override:
            remote_dir = remote_root.rstrip('/')
        else:
            safe_alias = self.instance.alias.replace(' ', '_')
            remote_dir = f"{remote_root.rstrip('/')}/{safe_alias}"
        executor.run(f"mkdir -p {shlex.quote(remote_dir)}")
        return f"{remote_dir}/{filename}"

    def _build_remote_path(self, filename: str, remote_root_override: str | None = None) -> str | None:
        remote_root = (remote_root_override or self.instance.remote_backup_root or '').strip()
        if not remote_root:
            return None
        if remote_root_override:
            remote_dir = remote_root.rstrip('/')
        else:
            safe_alias = self.instance.alias.replace(' ', '_')
            remote_dir = f"{remote_root.rstrip('/')}/{safe_alias}"
        return f"{remote_dir}/{filename}"

    def _upload_to_remote(
        self,
        local_path: Path,
        filename: str,
        remote_root_override: str | None = None,
        remote_config: dict | None = None
    ) -> str | None:
        if remote_config:
            protocol = (remote_config.get('protocol') or 'ssh').lower()
            remote_path = self._build_remote_path(filename, remote_root_override)
            if not remote_path:
                return None
            if protocol == 'ssh':
                executor = RemoteExecutor(
                    host=remote_config.get('host'),
                    port=remote_config.get('port'),
                    user=remote_config.get('user'),
                    password=remote_config.get('password'),
                    key_path=remote_config.get('key_path')
                )
                remote_dir = str(Path(remote_path).parent).replace('\\', '/')
                executor.run(f"mkdir -p {shlex.quote(remote_dir)}")
                executor.upload(local_path, remote_path)
                return remote_path
            client = RemoteStorageClient(
                protocol=protocol,
                host=remote_config.get('host'),
                port=remote_config.get('port'),
                user=remote_config.get('user'),
                password=remote_config.get('password'),
                key_path=remote_config.get('key_path')
            )
            return client.upload(local_path, remote_path)

        executor = RemoteExecutor(self.instance)
        if not executor._is_remote():
            raise RuntimeError('未配置 SSH 连接信息，无法写入 MySQL 服务器路径')
        remote_path = self._get_remote_backup_path(filename, executor, remote_root_override)
        if not remote_path:
            return None
        executor.upload(local_path, remote_path)
        return remote_path

    def _upload_to_object_storage(self, local_path: Path, filename: str, config: dict | None = None) -> str | None:
        uploader = ObjectStorageUploader(config=config)
        try:
            return uploader.upload(local_path, self.instance.alias, filename)
        except Exception as exc:
            logger.warning(f"OSS 上传失败: {exc}")
            return None
    
    def execute_backup(
        self,
        database_name=None,
        compress=True,
        storage_path=None,
        backup_type='full',
        base_backup=None,
        store_local=True,
        store_remote=False,
        store_oss=False,
        remote_storage_path=None,
        remote_config=None,
        oss_config=None
    ):
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
                    database_name,
                    compress,
                    storage_path,
                    filename,
                    store_local,
                    store_remote,
                    store_oss,
                    remote_storage_path,
                    remote_config,
                    oss_config
                )

            if backup_type in ['hot']:
                return self._execute_hot_backup(
                    storage_path,
                    timestamp,
                    compress,
                    store_local,
                    store_remote,
                    store_oss,
                    remote_storage_path,
                    remote_config,
                    oss_config
                )

            if backup_type in ['cold']:
                return self._execute_cold_backup(
                    storage_path,
                    timestamp,
                    compress,
                    store_local,
                    store_remote,
                    store_oss,
                    remote_storage_path,
                    remote_config,
                    oss_config
                )

            if backup_type in ['incremental']:
                return self._execute_incremental_backup(
                    storage_path,
                    timestamp,
                    compress,
                    base_backup,
                    store_local,
                    store_remote,
                    store_oss,
                    remote_storage_path,
                    remote_config,
                    oss_config
                )

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

    def _get_user_databases(self) -> list[str]:
        """获取非系统库列表，避免备份/恢复 mysql 系统表引发错误。"""
        system_dbs = {'information_schema', 'mysql', 'performance_schema', 'sys'}
        dbs = []
        try:
            connection = self.instance.get_connection()
            with connection.cursor() as cursor:
                cursor.execute('SHOW DATABASES')
                for row in cursor.fetchall():
                    name = row.get('Database') if isinstance(row, dict) else row[0]
                    if name and name.lower() not in system_dbs:
                        dbs.append(name)
            connection.close()
        except Exception as exc:
            logger.error(f"获取数据库列表失败: {exc}")
        return dbs

    def _supports_ssl_mode(self, dump_bin: str) -> bool:
        """检测 mysqldump 是否支持 --ssl-mode 选项。"""
        try:
            result = subprocess.run(
                [dump_bin, '--help'],
                capture_output=True,
                text=True,
                timeout=5
            )
            output = f"{result.stdout}\n{result.stderr}"
            return 'ssl-mode' in output
        except Exception:
            return False

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

        # SSL/TLS 配置（默认禁用以兼容自签名证书）
        ssl_mode = getattr(settings, 'MYSQL_DUMP_SSL_MODE', 'DISABLED')
        if ssl_mode:
            if self._supports_ssl_mode(dump_bin):
                cmd_parts.append(f'--ssl-mode={ssl_mode}')
            elif str(ssl_mode).upper() in ('DISABLED', 'DISABLE', 'OFF', '0'):
                cmd_parts.append('--skip-ssl')
            else:
                logger.warning(
                    "mysqldump 不支持 --ssl-mode，已忽略 SSL 配置: %s",
                    ssl_mode
                )

        ssl_ca = getattr(settings, 'MYSQL_DUMP_SSL_CA', '')
        if ssl_ca:
            cmd_parts.append(f'--ssl-ca="{ssl_ca}"')
        
        # 添加常用选项
        cmd_parts.extend([
            '--single-transaction',  # 对于InnoDB，保证一致性备份
            '--quick',  # 快速导出，不缓冲到内存
            '--lock-tables=false',  # 不锁表
        ])
        
        # 指定数据库
        if database_name:
            cmd_parts.append(f'--databases {shlex.quote(database_name)}')
        else:
            include_system = getattr(settings, 'MYSQL_DUMP_INCLUDE_SYSTEM_DATABASES', False)
            if include_system:
                cmd_parts.append('--all-databases')
            else:
                dbs = self._get_user_databases()
                if not dbs:
                    raise ValueError('未找到可备份的非系统数据库')
                safe_dbs = ' '.join(shlex.quote(db) for db in dbs)
                cmd_parts.append(f'--databases {safe_dbs}')
        
        # 输出重定向
        cmd = ' '.join(cmd_parts) + f' > "{output_file}"'
        
        return cmd

    def _execute_logical_backup(
        self,
        database_name,
        compress,
        storage_path,
        filename,
        store_local,
        store_remote,
        store_oss,
        remote_storage_path,
        remote_config,
        oss_config
    ):
        """执行逻辑备份（mysqldump）"""
        dump_bin = self._get_dump_binary()
        if not dump_bin:
            return {
                'success': False,
                'error_message': 'mysqldump 或 mariadb-dump 未安装'
            }

        file_path = storage_path / filename
        try:
            dump_cmd = self._build_mysqldump_command(database_name, str(file_path), dump_bin)
        except ValueError as exc:
            return {
                'success': False,
                'error_message': str(exc)
            }

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
        remote_error = None
        if store_remote:
            try:
                remote_path = self._upload_to_remote(
                    final_path,
                    final_path.name,
                    remote_storage_path,
                    remote_config
                )
            except Exception as exc:
                remote_error = str(exc)
                logger.warning(f"远程备份上传失败: {exc}")

        object_storage_path = ''
        if store_oss:
            object_storage_path = self._upload_to_object_storage(
                final_path,
                final_path.name,
                config=oss_config
            ) or ''

        if store_remote and not remote_path:
            return {
                'success': False,
                'error_message': remote_error or '远程备份上传失败，请检查远程路径与 SSH 配置'
            }

        if not store_local:
            if final_path.exists():
                final_path.unlink()
            final_path = Path('')

        return {
            'success': True,
            'file_path': str(final_path) if str(final_path) else '',
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
        return f"/tmp/auroravault/{safe_alias}"

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

    def _execute_hot_backup(
        self,
        storage_path,
        timestamp,
        compress,
        store_local,
        store_remote,
        store_oss,
        remote_storage_path,
        remote_config,
        oss_config
    ):
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
        remote_keep_path = None
        if store_remote and not remote_config:
            remote_keep_path = self._get_remote_backup_path(
                archive_name,
                executor,
                remote_storage_path
            )
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
        if store_remote and remote_config:
            try:
                remote_keep_path = self._upload_to_remote(
                    local_path,
                    local_path.name,
                    remote_storage_path,
                    remote_config
                )
            except Exception as exc:
                logger.warning(f"远程备份上传失败: {exc}")
        object_storage_path = ''
        if store_oss:
            object_storage_path = self._upload_to_object_storage(
                local_path,
                local_path.name,
                config=oss_config
            ) or ''

        if not store_local and local_path.exists():
            local_path.unlink()
            local_path = Path('')
        return {
            'success': True,
            'file_path': str(local_path) if str(local_path) else '',
            'file_size_mb': round(file_size_mb, 2),
            'remote_path': remote_keep_path or '',
            'object_storage_path': object_storage_path or ''
        }

    def _execute_incremental_backup(
        self,
        storage_path,
        timestamp,
        compress,
        base_backup,
        store_local,
        store_remote,
        store_oss,
        remote_storage_path,
        remote_config,
        oss_config
    ):
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
        remote_keep_path = None
        if store_remote and not remote_config:
            remote_keep_path = self._get_remote_backup_path(
                archive_name,
                executor,
                remote_storage_path
            )
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
        if store_remote and remote_config:
            try:
                remote_keep_path = self._upload_to_remote(
                    local_path,
                    local_path.name,
                    remote_storage_path,
                    remote_config
                )
            except Exception as exc:
                logger.warning(f"远程备份上传失败: {exc}")
        object_storage_path = ''
        if store_oss:
            object_storage_path = self._upload_to_object_storage(
                local_path,
                local_path.name,
                config=oss_config
            ) or ''

        if not store_local and local_path.exists():
            local_path.unlink()
            local_path = Path('')
        return {
            'success': True,
            'file_path': str(local_path) if str(local_path) else '',
            'file_size_mb': round(file_size_mb, 2),
            'remote_path': remote_keep_path or '',
            'object_storage_path': object_storage_path or ''
        }

    def _execute_cold_backup(
        self,
        storage_path,
        timestamp,
        compress,
        store_local,
        store_remote,
        store_oss,
        remote_storage_path,
        remote_config,
        oss_config
    ):
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

        remote_keep_path = None
        if store_remote and not remote_config:
            remote_keep_path = self._get_remote_backup_path(
                archive_name,
                executor,
                remote_storage_path
            )
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
        if store_remote and remote_config:
            try:
                remote_keep_path = self._upload_to_remote(
                    local_path,
                    local_path.name,
                    remote_storage_path,
                    remote_config
                )
            except Exception as exc:
                logger.warning(f"远程备份上传失败: {exc}")
        object_storage_path = ''
        if store_oss:
            object_storage_path = self._upload_to_object_storage(
                local_path,
                local_path.name,
                config=oss_config
            ) or ''

        if not store_local and local_path.exists():
            local_path.unlink()
            local_path = Path('')
        return {
            'success': True,
            'file_path': str(local_path) if str(local_path) else '',
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

    def _supports_ssl_mode(self, mysql_bin: str) -> bool:
        """检测 mysql 是否支持 --ssl-mode 选项。"""
        try:
            result = subprocess.run(
                [mysql_bin, '--help'],
                capture_output=True,
                text=True,
                timeout=5
            )
            output = f"{result.stdout}\n{result.stderr}"
            return 'ssl-mode' in output
        except Exception:
            return False
    
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
        mysql_bin = 'mysql'
        cmd_parts = [
            mysql_bin,
            f'-h {self.instance.host}',
            f'-P {self.instance.port}',
            f'-u {self.instance.username}',
        ]
        
        # 添加密码
        if password:
            cmd_parts.append(f'-p"{password}"')

        # SSL/TLS 配置（默认禁用以兼容自签名证书）
        ssl_mode = getattr(settings, 'MYSQL_DUMP_SSL_MODE', 'DISABLED')
        if ssl_mode:
            if self._supports_ssl_mode(mysql_bin):
                cmd_parts.append(f'--ssl-mode={ssl_mode}')
                ssl_ca = getattr(settings, 'MYSQL_DUMP_SSL_CA', '')
                if ssl_ca:
                    cmd_parts.append(f'--ssl-ca="{ssl_ca}"')
            elif str(ssl_mode).upper() in ('DISABLED', 'DISABLE', 'OFF', '0'):
                cmd_parts.append('--skip-ssl')
            else:
                logger.warning(
                    "mysql 不支持 --ssl-mode，已忽略 SSL 配置: %s",
                    ssl_mode
                )
        
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
