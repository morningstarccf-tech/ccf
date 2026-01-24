"""
MySQL 实例管理的服务层代码

提供连接池管理、健康检查和监控指标采集等核心服务。
"""
import time
import logging
import re
import shlex
from typing import Optional, Dict, Any, Tuple
from contextlib import contextmanager
from django.utils import timezone
import pymysql
from pymysql.cursors import DictCursor
from apps.backups.services import RemoteExecutor

logger = logging.getLogger(__name__)


class ConnectionPoolManager:
    """
    MySQL 连接池管理器
    
    为每个 MySQL 实例维护一个连接池，支持连接复用和自动清理。
    使用单例模式确保全局只有一个连接池管理器。
    """
    
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._pools = {}
            cls._instance._pool_configs = {}
        return cls._instance
    
    def get_pool_key(self, instance_id: int) -> str:
        """
        获取连接池的唯一标识
        
        Args:
            instance_id: 实例 ID
            
        Returns:
            str: 连接池键名
        """
        return f"instance_{instance_id}"
    
    def create_pool(self, instance_id: int, host: str, port: int, 
                   user: str, password: str, charset: str = 'utf8mb4',
                   max_connections: int = 10) -> None:
        """
        创建连接池
        
        Args:
            instance_id: 实例 ID
            host: 主机地址
            port: 端口号
            user: 用户名
            password: 密码
            charset: 字符集
            max_connections: 最大连接数
        """
        pool_key = self.get_pool_key(instance_id)
        
        if pool_key in self._pools:
            logger.info(f"Connection pool {pool_key} already exists")
            return
        
        self._pool_configs[pool_key] = {
            'host': host,
            'port': port,
            'user': user,
            'password': password,
            'charset': charset,
            'max_connections': max_connections,
            'cursorclass': DictCursor,
        }
        
        # 初始化连接列表
        self._pools[pool_key] = {
            'connections': [],
            'in_use': 0,
            'created_at': timezone.now(),
        }
        
        logger.info(f"Created connection pool {pool_key}")
    
    def get_connection(self, instance_id: int):
        """
        从连接池获取连接
        
        Args:
            instance_id: 实例 ID
            
        Returns:
            pymysql.Connection: 数据库连接对象
            
        Raises:
            ValueError: 连接池不存在
            pymysql.Error: 连接失败
        """
        pool_key = self.get_pool_key(instance_id)
        
        if pool_key not in self._pools:
            raise ValueError(f"Connection pool {pool_key} does not exist")
        
        pool = self._pools[pool_key]
        config = self._pool_configs[pool_key]
        
        # 尝试从池中获取可用连接
        for conn in pool['connections']:
            try:
                # 检查连接是否有效
                conn.ping(reconnect=True)
                pool['in_use'] += 1
                return conn
            except:
                # 连接已失效，移除
                pool['connections'].remove(conn)
        
        # 如果池中没有可用连接，创建新连接
        if pool['in_use'] < config['max_connections']:
            # 在最大连接数内创建新的池连接。
            conn = pymysql.connect(
                host=config['host'],
                port=config['port'],
                user=config['user'],
                password=config['password'],
                charset=config['charset'],
                cursorclass=config['cursorclass']
            )
            pool['connections'].append(conn)
            pool['in_use'] += 1
            logger.debug(f"Created new connection for pool {pool_key}")
            return conn
        
        # 连接池已满，创建临时连接
        logger.warning(f"Connection pool {pool_key} is full, creating temporary connection")
        # 临时连接不计入连接池使用计数。
        return pymysql.connect(
            host=config['host'],
            port=config['port'],
            user=config['user'],
            password=config['password'],
            charset=config['charset'],
            cursorclass=config['cursorclass']
        )
    
    def release_connection(self, instance_id: int) -> None:
        """
        释放连接回连接池
        
        Args:
            instance_id: 实例 ID
        """
        pool_key = self.get_pool_key(instance_id)
        
        if pool_key in self._pools:
            pool = self._pools[pool_key]
            if pool['in_use'] > 0:
                # 仅在存在使用中的连接时才减少计数。
                pool['in_use'] -= 1
    
    def close_pool(self, instance_id: int) -> None:
        """
        关闭并清理连接池
        
        Args:
            instance_id: 实例 ID
        """
        pool_key = self.get_pool_key(instance_id)
        
        if pool_key in self._pools:
            pool = self._pools[pool_key]
            
            # 关闭所有连接
            for conn in pool['connections']:
                try:
                    conn.close()
                except:
                    pass
            
            # 清理连接池
            del self._pools[pool_key]
            del self._pool_configs[pool_key]
            logger.info(f"Closed connection pool {pool_key}")
    
    @contextmanager
    def get_connection_context(self, instance_id: int):
        """
        上下文管理器方式获取连接
        
        Args:
            instance_id: 实例 ID
            
        Yields:
            pymysql.Connection: 数据库连接
        """
        conn = None
        try:
            conn = self.get_connection(instance_id)
            yield conn
        finally:
            if conn:
                self.release_connection(instance_id)


class HealthChecker:
    """
    MySQL 实例健康检查服务
    
    检查实例的连接状态、版本信息和基本性能指标。
    """
    
    @staticmethod
    def check_instance(instance) -> Tuple[bool, str, Dict[str, Any]]:
        """
        检查实例健康状态
        
        Args:
            instance: MySQLInstance 实例
            
        Returns:
            tuple: (是否健康, 消息, 额外信息)
        """
        start_time = time.time()
        info = {}
        
        try:
            # 测试连接
            connection = pymysql.connect(
                host=instance.host,
                port=instance.port,
                user=instance.username,
                password=instance.get_decrypted_password(),
                connect_timeout=5
            )
            
            response_time = (time.time() - start_time) * 1000  # 毫秒
            
            with connection.cursor() as cursor:
                # 获取版本
                cursor.execute("SELECT VERSION()")
                version = cursor.fetchone()[0]
                info['version'] = version
                
                # 获取字符集
                cursor.execute("SHOW VARIABLES LIKE 'character_set_server'")
                charset_result = cursor.fetchone()
                if charset_result:
                    info['charset'] = charset_result[1]
                
                # 获取当前连接数
                cursor.execute("SHOW STATUS LIKE 'Threads_connected'")
                connections_result = cursor.fetchone()
                if connections_result:
                    info['connections'] = int(connections_result[1])
                
                # 获取运行时间
                cursor.execute("SHOW STATUS LIKE 'Uptime'")
                uptime_result = cursor.fetchone()
                if uptime_result:
                    info['uptime_seconds'] = int(uptime_result[1])
            
            connection.close()
            
            info['response_time_ms'] = round(response_time, 2)
            
            return True, f"连接成功，响应时间: {info['response_time_ms']}ms", info
        
        except pymysql.OperationalError as e:
            error_msg = f"连接失败: {str(e)}"
            logger.error(f"Instance {instance.alias} health check failed: {error_msg}")
            return False, error_msg, {'error': str(e)}
        
        except Exception as e:
            error_msg = f"健康检查异常: {str(e)}"
            logger.exception(f"Instance {instance.alias} health check error: {error_msg}")
            return False, error_msg, {'error': str(e)}


class MetricsCollector:
    """
    监控指标采集器
    
    采集 MySQL 实例的性能监控数据，包括 QPS、TPS、连接数等。
    """
    
    @staticmethod
    def collect_metrics(instance) -> Optional[Dict[str, Any]]:
        """
        采集实例监控指标
        
        Args:
            instance: MySQLInstance 实例
            
        Returns:
            dict: 监控指标数据，失败返回 None
        """
        try:
            connection = instance.get_connection()
            metrics = {}
            
            with connection.cursor() as cursor:
                # 获取状态变量
                cursor.execute("SHOW GLOBAL STATUS")
                status_vars = {row['Variable_name']: row['Value'] for row in cursor.fetchall()}
                
                # 计算 QPS (Queries Per Second)
                # 需要两次采样计算差值
                queries = int(status_vars.get('Queries', 0))
                uptime = int(status_vars.get('Uptime', 1))
                metrics['qps'] = round(queries / uptime, 2) if uptime > 0 else 0
                
                # 计算 TPS (Transactions Per Second)
                com_commit = int(status_vars.get('Com_commit', 0))
                com_rollback = int(status_vars.get('Com_rollback', 0))
                transactions = com_commit + com_rollback
                metrics['tps'] = round(transactions / uptime, 2) if uptime > 0 else 0
                
                # 当前连接数
                metrics['connections'] = int(status_vars.get('Threads_connected', 0))
                
                # 慢查询数
                metrics['slow_queries'] = int(status_vars.get('Slow_queries', 0))
                
                # 获取进程列表信息
                cursor.execute("SHOW PROCESSLIST")
                processlist = cursor.fetchall()
                metrics['active_connections'] = len([p for p in processlist if p['Command'] != 'Sleep'])
                
                # 获取数据库大小
                cursor.execute("""
                    SELECT SUM(data_length + index_length) / 1024 / 1024 as size_mb
                    FROM information_schema.TABLES
                """)
                size_result = cursor.fetchone()
                metrics['total_size_mb'] = round(size_result['size_mb'], 2) if size_result['size_mb'] else 0
                
                # 获取系统变量
                cursor.execute("SHOW VARIABLES LIKE 'max_connections'")
                max_conn = cursor.fetchone()
                if max_conn:
                    metrics['max_connections'] = int(max_conn['Value'])
                    metrics['connection_usage_percent'] = round(
                        (metrics['connections'] / metrics['max_connections']) * 100, 2
                    )
            
            connection.close()
            
            system_metrics = MetricsCollector._collect_system_metrics(instance)
            if system_metrics:
                # 合并通过 SSH 采集的系统指标（CPU/内存/磁盘）。
                metrics.update(system_metrics)
            else:
                metrics['cpu_usage'] = 0
                metrics['memory_usage'] = 0
                metrics['disk_usage'] = 0
            
            return metrics
        
        except Exception as e:
            logger.error(f"Failed to collect metrics for {instance.alias}: {str(e)}")
            return None
    
    @staticmethod
    def save_metrics(instance, metrics: Dict[str, Any]) -> bool:
        """
        保存监控指标到数据库
        
        Args:
            instance: MySQLInstance 实例
            metrics: 监控指标数据
            
        Returns:
            bool: 是否保存成功
        """
        try:
            from apps.instances.models import MonitoringMetrics
            
            MonitoringMetrics.objects.create(
                instance=instance,
                qps=metrics.get('qps', 0),
                tps=metrics.get('tps', 0),
                connections=metrics.get('connections', 0),
                slow_queries=metrics.get('slow_queries', 0),
                cpu_usage=metrics.get('cpu_usage', 0),
                memory_usage=metrics.get('memory_usage', 0),
                disk_usage=metrics.get('disk_usage', 0),
            )
            
            logger.info(f"Saved metrics for instance {instance.alias}")
            return True
        
        except Exception as e:
            logger.error(f"Failed to save metrics for {instance.alias}: {str(e)}")
            return False

    @staticmethod
    def _collect_system_metrics(instance) -> Optional[Dict[str, Any]]:
        """通过 SSH 采集 CPU/内存/磁盘使用率。"""
        executor = RemoteExecutor(instance)
        if not executor._is_remote():
            return None

        metrics: Dict[str, Any] = {}

        # 处理器使用率
        code, stdout, _ = executor.run("LANG=C top -bn1 | grep 'Cpu(s)'")
        if code == 0 and stdout:
            idle_match = re.search(r'([0-9.]+)\\s*id', stdout)
            if idle_match:
                idle = float(idle_match.group(1))
                metrics['cpu_usage'] = round(max(0.0, 100.0 - idle), 2)

        if 'cpu_usage' not in metrics:
            code, stdout, _ = executor.run("LANG=C mpstat 1 1 | awk '/Average/ {print 100-$NF}'")
            if code == 0 and stdout:
                value = stdout.strip().splitlines()[-1].strip()
                if value.replace('.', '', 1).isdigit():
                    metrics['cpu_usage'] = round(float(value), 2)

        if 'cpu_usage' not in metrics:
            code, stdout, _ = executor.run("awk '/^cpu / {total=$2+$3+$4+$5+$6+$7+$8+$9; idle=$5; if (total>0) print (1-idle/total)*100;}' /proc/stat")
            if code == 0 and stdout:
                value = stdout.strip().splitlines()[-1].strip()
                if value.replace('.', '', 1).isdigit():
                    metrics['cpu_usage'] = round(float(value), 2)

        # 内存使用率
        code, stdout, _ = executor.run("LANG=C free -m | awk '/Mem:/ {print $2\" \"$3}'")
        if code == 0 and stdout:
            parts = stdout.strip().split()
            if len(parts) >= 2:
                total = float(parts[0])
                used = float(parts[1])
                if total > 0:
                    metrics['memory_usage'] = round((used / total) * 100, 2)

        if 'memory_usage' not in metrics:
            code, stdout, _ = executor.run("awk '/MemTotal/ {t=$2} /MemAvailable/ {a=$2} END {if (t>0) print (t-a)/t*100}' /proc/meminfo")
            if code == 0 and stdout:
                value = stdout.strip().splitlines()[-1].strip()
                if value.replace('.', '', 1).isdigit():
                    metrics['memory_usage'] = round(float(value), 2)

        # 磁盘使用率
        disk_path = instance.data_dir or '/'
        quoted_path = shlex.quote(disk_path)
        code, stdout, _ = executor.run(f"df -P {quoted_path} | tail -1")
        if code == 0 and stdout:
            parts = stdout.split()
            if len(parts) >= 5:
                percent = parts[4].strip().rstrip('%')
                if percent.replace('.', '', 1).isdigit():
                    metrics['disk_usage'] = round(float(percent), 2)

        if not metrics:
            return None

        metrics.setdefault('cpu_usage', 0)
        metrics.setdefault('memory_usage', 0)
        metrics.setdefault('disk_usage', 0)
        return metrics


class DatabaseSyncService:
    """同步实例数据库列表并刷新统计信息。"""

    @staticmethod
    def sync_databases(
        instance,
        refresh_stats: bool = True,
        include_system: bool = False,
        prune_missing: bool = True
    ) -> Dict[str, Any]:
        system_schemas = {'information_schema', 'mysql', 'performance_schema', 'sys'}

        # 从 information_schema 获取库信息。
        connection = instance.get_connection()
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT
                    SCHEMA_NAME AS name,
                    DEFAULT_CHARACTER_SET_NAME AS charset,
                    DEFAULT_COLLATION_NAME AS collation
                FROM information_schema.SCHEMATA
            """)
            schemas = cursor.fetchall()
        connection.close()

        from apps.instances.models import Database

        created_count = 0
        updated_count = 0
        deleted_count = 0
        synced = []

        for schema in schemas:
            name = (schema.get('name') or '').strip()
            if not name:
                continue
            if not include_system and name in system_schemas:
                continue

            defaults = {
                'charset': schema.get('charset') or instance.charset,
                'collation': schema.get('collation') or 'utf8mb4_unicode_ci',
            }
            database, created = Database.objects.update_or_create(
                instance=instance,
                name=name,
                defaults=defaults
            )
            if created:
                created_count += 1
            else:
                updated_count += 1

            if refresh_stats:
                try:
                    # 根据需要刷新库表数量与大小统计。
                    database.update_statistics()
                except Exception as exc:
                    logger.warning(f"Failed to refresh stats for {database}: {exc}")

            synced.append(name)

        if prune_missing:
            # 删除本地记录中已不存在的数据库。
            queryset = Database.objects.filter(instance=instance)
            if synced:
                queryset = queryset.exclude(name__in=synced)
            deleted_count, _ = queryset.delete()

        return {
            'created': created_count,
            'updated': updated_count,
            'deleted': deleted_count,
            'total': len(synced),
            'databases': synced,
        }


# 全局连接池管理器实例
connection_pool_manager = ConnectionPoolManager()
