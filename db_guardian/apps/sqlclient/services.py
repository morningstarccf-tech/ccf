"""
SQL客户端应用服务层
提供SQL执行、模式浏览、结果导出等核心业务逻辑
"""
import time
import csv
import io
from typing import Dict, Any, Optional, List
from django.core.cache import cache
from django.utils import timezone
import pymysql
import logging

from apps.instances.models import MySQLInstance
from apps.authentication.models import User
from .models import QueryHistory
from .validators import SQLValidator

logger = logging.getLogger(__name__)


class QueryExecutor:
    """
    SQL查询执行器
    
    负责执行SQL查询，包括权限检查、超时控制、结果缓存等功能
    """
    
    def __init__(self, instance: MySQLInstance, user: User):
        """
        初始化查询执行器
        
        Args:
            instance: MySQL实例
            user: 执行用户
        """
        self.instance = instance
        self.user = user
        self.validator = SQLValidator()
    
    def execute_query(
        self, 
        sql: str, 
        database: Optional[str] = None, 
        timeout: int = 30,
        apply_limit: bool = True,
        max_rows: int = 1000
    ) -> Dict[str, Any]:
        """
        执行SQL查询
        
        Args:
            sql: SQL语句
            database: 数据库名称
            timeout: 超时时间（秒）
            apply_limit: 是否自动应用行数限制（仅SELECT）
            max_rows: 最大返回行数
            
        Returns:
            dict: 执行结果
            {
                'success': bool,
                'sql_type': str,
                'rows_affected': int,
                'execution_time_ms': int,
                'columns': List[str],
                'data': List[dict],
                'message': str,
                'history_id': int,
                'warnings': List[str]
            }
        """
        start_time = time.time()
        result = {
            'success': False,
            'sql_type': 'UNKNOWN',
            'rows_affected': 0,
            'execution_time_ms': 0,
            'columns': [],
            'data': [],
            'message': '',
            'history_id': None,
            'warnings': []
        }
        
        connection = None
        history = None
        
        try:
            # 1. 验证SQL
            validation_result = self._validate_sql(sql)
            if not validation_result['is_valid']:
                result['message'] = validation_result['message']
                # 记录失败的执行历史
                self._create_history(
                    database, sql, validation_result['sql_type'],
                    'failed', 0, 0, result['message']
                )
                return result
            
            sql_type = validation_result['sql_type']
            result['sql_type'] = sql_type
            result['warnings'].extend(validation_result.get('warnings', []))
            
            # 2. 检查权限
            permission_check = self._check_permission(sql_type)
            if not permission_check['allowed']:
                result['message'] = permission_check['message']
                self._create_history(
                    database, sql, sql_type,
                    'failed', 0, 0, result['message']
                )
                return result
            
            # 3. 应用行数限制（仅对SELECT查询）
            if apply_limit and sql_type == 'SELECT':
                sql = self.validator.apply_row_limit(sql, max_rows)
                if max_rows < 1000:
                    result['warnings'].append(f'已自动限制返回行数为 {max_rows}')
            
            # 4. 获取数据库连接
            connection = self._get_connection(database)
            
            # 5. 设置查询超时
            with connection.cursor() as cursor:
                # 设置语句超时（MySQL 5.7.4+）
                try:
                    cursor.execute(f"SET SESSION max_execution_time = {timeout * 1000}")
                except pymysql.Error:
                    # 如果不支持 max_execution_time，忽略错误
                    pass
                
                # 6. 执行查询
                cursor.execute(sql)
                
                # 7. 获取结果
                if sql_type in ['SELECT', 'SHOW', 'DESC', 'EXPLAIN']:
                    # 查询类SQL，获取结果集
                    data = cursor.fetchall()
                    result['data'] = data
                    result['rows_affected'] = len(data)
                    
                    # 获取列名
                    if cursor.description:
                        result['columns'] = [desc[0] for desc in cursor.description]
                else:
                    # 修改类SQL，获取影响行数
                    result['rows_affected'] = cursor.rowcount
                    connection.commit()
            
            # 8. 计算执行时间
            execution_time_ms = int((time.time() - start_time) * 1000)
            result['execution_time_ms'] = execution_time_ms
            
            # 9. 记录执行历史
            history = self._create_history(
                database, sql, sql_type,
                'success', result['rows_affected'], execution_time_ms, ''
            )
            result['history_id'] = history.id
            
            result['success'] = True
            result['message'] = '执行成功'

            # 10. 缓存结果（仅查询类SQL）
            if sql_type in ['SELECT', 'SHOW', 'DESC', 'EXPLAIN'] and data:
                cache_data = {
                    'columns': result['columns'],
                    'data': result['data'],
                    'rows_affected': result['rows_affected'],
                    'sql_type': sql_type
                }
                try:
                    history.cache_result(cache_data)
                except Exception as cache_error:
                    cache_msg = f'结果缓存失败: {cache_error}'
                    result['warnings'].append(cache_msg)
                    logger.warning(cache_msg)
            
        except pymysql.Error as e:
            # MySQL错误
            error_msg = f'MySQL错误: {str(e)}'
            result['message'] = error_msg
            execution_time_ms = int((time.time() - start_time) * 1000)
            result['execution_time_ms'] = execution_time_ms
            
            # 判断是否超时
            status = 'timeout' if 'timeout' in str(e).lower() else 'failed'
            
            self._create_history(
                database, sql, result['sql_type'],
                status, 0, execution_time_ms, error_msg
            )
            
            logger.error(f"Query execution failed for user {self.user.username}: {error_msg}")
            
        except Exception as e:
            # 其他异常
            error_msg = f'执行异常: {str(e)}'
            result['message'] = error_msg
            execution_time_ms = int((time.time() - start_time) * 1000)
            result['execution_time_ms'] = execution_time_ms
            
            self._create_history(
                database, sql, result['sql_type'],
                'failed', 0, execution_time_ms, error_msg
            )
            
            logger.exception(f"Query execution error for user {self.user.username}: {error_msg}")
            
        finally:
            if connection:
                connection.close()
        
        return result
    
    def _validate_sql(self, sql: str) -> Dict[str, Any]:
        """
        验证SQL语句
        
        Args:
            sql: SQL语句
            
        Returns:
            dict: 验证结果
        """
        # 确定允许的SQL类型
        allowed_types = self._get_allowed_sql_types()
        return self.validator.validate_sql(sql, allowed_types)
    
    def _get_allowed_sql_types(self) -> List[str]:
        """
        获取用户允许执行的SQL类型
        
        Returns:
            List[str]: 允许的SQL类型列表
        """
        if self.user.is_superuser:
            # 超级管理员允许所有类型
            return (SQLValidator.ALLOWED_QUERY_TYPES + 
                   SQLValidator.ALLOWED_MODIFY_TYPES + 
                   SQLValidator.ALLOWED_DDL_TYPES)
        
        # 获取用户在团队中的权限
        allowed = []
        team = self.instance.team
        
        # 查询权限
        if self.user.has_team_permission(team, 'execute_sql_query'):
            allowed.extend(SQLValidator.ALLOWED_QUERY_TYPES)
        
        # 修改权限
        if self.user.has_team_permission(team, 'execute_sql_modify'):
            allowed.extend(SQLValidator.ALLOWED_MODIFY_TYPES)
        
        # DDL权限
        if self.user.has_team_permission(team, 'execute_ddl'):
            allowed.extend(SQLValidator.ALLOWED_DDL_TYPES)
        
        return allowed
    
    def _check_permission(self, sql_type: str) -> Dict[str, bool]:
        """
        检查用户是否有权限执行此类型的SQL
        
        Args:
            sql_type: SQL类型
            
        Returns:
            dict: {'allowed': bool, 'message': str}
        """
        if self.user.is_superuser:
            return {'allowed': True, 'message': ''}
        
        team = self.instance.team
        
        # 判断需要的权限
        if sql_type in SQLValidator.ALLOWED_QUERY_TYPES:
            required_perm = 'execute_sql_query'
        elif sql_type in SQLValidator.ALLOWED_MODIFY_TYPES:
            required_perm = 'execute_sql_modify'
        elif sql_type in SQLValidator.ALLOWED_DDL_TYPES:
            required_perm = 'execute_ddl'
        else:
            return {'allowed': False, 'message': f'不支持的SQL类型: {sql_type}'}
        
        # 检查权限
        if self.user.has_team_permission(team, required_perm):
            return {'allowed': True, 'message': ''}
        else:
            return {'allowed': False, 'message': f'您没有执行 {sql_type} 类型SQL的权限'}
    
    def _get_connection(self, database: Optional[str] = None):
        """
        获取数据库连接
        
        Args:
            database: 数据库名称
            
        Returns:
            pymysql.Connection: 数据库连接
        """
        conn_params = {
            'host': self.instance.host,
            'port': self.instance.port,
            'user': self.instance.username,
            'password': self.instance.get_decrypted_password(),
            'charset': self.instance.charset,
            'cursorclass': pymysql.cursors.DictCursor
        }
        
        if database:
            conn_params['database'] = database
        
        return pymysql.connect(**conn_params)
    
    def _create_history(
        self, 
        database: str, 
        sql: str, 
        sql_type: str,
        status: str, 
        rows_affected: int, 
        execution_time_ms: int,
        error_message: str
    ) -> Optional[QueryHistory]:
        """
        创建执行历史记录
        
        Args:
            database: 数据库名称
            sql: SQL语句
            sql_type: SQL类型
            status: 执行状态
            rows_affected: 影响行数
            execution_time_ms: 执行时间（毫秒）
            error_message: 错误信息
            
        Returns:
            QueryHistory: 历史记录对象
        """
        try:
            return QueryHistory.objects.create(
                instance=self.instance,
                database_name=database or '',
                sql_statement=sql,
                sql_type=sql_type,
                status=status,
                rows_affected=rows_affected,
                execution_time_ms=execution_time_ms,
                error_message=error_message,
                executed_by=self.user
            )
        except Exception as e:
            logger.error(f"Failed to create query history: {str(e)}")
            return None


class SchemaExplorer:
    """
    数据库模式浏览器
    
    提供数据库结构的查询和浏览功能
    """
    
    def __init__(self, instance: MySQLInstance):
        """
        初始化模式浏览器
        
        Args:
            instance: MySQL实例
        """
        self.instance = instance
    
    def get_database_schema(self, database: Optional[str] = None) -> Dict[str, Any]:
        """
        获取数据库结构
        
        Args:
            database: 数据库名称，为空则获取所有数据库
            
        Returns:
            dict: 数据库结构信息
            {
                'databases': [
                    {
                        'name': 'db1',
                        'tables': [
                            {
                                'name': 'users',
                                'type': 'BASE TABLE',
                                'engine': 'InnoDB',
                                'rows': 1000,
                                'columns': [...],
                                'indexes': [...]
                            }
                        ]
                    }
                ]
            }
        """
        try:
            connection = self.instance.get_connection()
            
            if database:
                # 获取指定数据库的结构
                databases = [self._get_database_info(connection, database)]
            else:
                # 获取所有数据库
                databases = self._get_all_databases(connection)
            
            connection.close()
            
            return {'databases': databases}
            
        except Exception as e:
            logger.error(f"Failed to get schema for {self.instance.alias}: {str(e)}")
            return {'databases': [], 'error': str(e)}
    
    def _get_all_databases(self, connection) -> List[Dict[str, Any]]:
        """获取所有数据库列表"""
        databases = []
        
        with connection.cursor() as cursor:
            cursor.execute("SHOW DATABASES")
            db_list = cursor.fetchall()
            
            for db in db_list:
                db_name = db['Database']
                # 跳过系统数据库
                if db_name in ['information_schema', 'mysql', 'performance_schema', 'sys']:
                    continue
                
                databases.append(self._get_database_info(connection, db_name))
        
        return databases
    
    def _get_database_info(self, connection, database: str) -> Dict[str, Any]:
        """获取单个数据库的详细信息"""
        db_info = {
            'name': database,
            'tables': []
        }
        
        # 获取表列表
        with connection.cursor() as cursor:
            cursor.execute(f"SHOW TABLES FROM `{database}`")
            tables = cursor.fetchall()
            
            for table in tables:
                table_name = table[f'Tables_in_{database}']
                table_info = self._get_table_info(connection, database, table_name)
                db_info['tables'].append(table_info)
        
        return db_info
    
    def _get_table_info(self, connection, database: str, table: str) -> Dict[str, Any]:
        """获取表的详细信息"""
        table_info = {
            'name': table,
            'type': 'BASE TABLE',
            'engine': '',
            'rows': 0,
            'columns': [],
            'indexes': []
        }
        
        with connection.cursor() as cursor:
            # 获取表状态
            cursor.execute(f"SHOW TABLE STATUS FROM `{database}` LIKE '{table}'")
            status = cursor.fetchone()
            if status:
                table_info['engine'] = status.get('Engine', '')
                table_info['rows'] = status.get('Rows', 0)
                table_info['type'] = status.get('Comment', 'BASE TABLE')
            
            # 获取列信息
            cursor.execute(f"SHOW FULL COLUMNS FROM `{database}`.`{table}`")
            columns = cursor.fetchall()
            for col in columns:
                table_info['columns'].append({
                    'name': col['Field'],
                    'type': col['Type'],
                    'null': col['Null'],
                    'key': col['Key'],
                    'default': col['Default'],
                    'extra': col['Extra'],
                    'comment': col.get('Comment', '')
                })
            
            # 获取索引信息
            cursor.execute(f"SHOW INDEX FROM `{database}`.`{table}`")
            indexes = cursor.fetchall()
            index_dict = {}
            for idx in indexes:
                index_name = idx['Key_name']
                if index_name not in index_dict:
                    index_dict[index_name] = {
                        'name': index_name,
                        'unique': not idx['Non_unique'],
                        'columns': []
                    }
                index_dict[index_name]['columns'].append(idx['Column_name'])
            
            table_info['indexes'] = list(index_dict.values())
        
        return table_info


class ResultExporter:
    """
    查询结果导出器
    
    支持将查询结果导出为CSV等格式
    """
    
    @staticmethod
    def export_to_csv(result_data: Dict[str, Any]) -> str:
        """
        导出结果为CSV格式
        
        Args:
            result_data: 查询结果数据
            {
                'columns': List[str],
                'data': List[dict]
            }
            
        Returns:
            str: CSV格式的字符串
        """
        output = io.StringIO()
        
        columns = result_data.get('columns', [])
        data = result_data.get('data', [])
        
        if not columns or not data:
            return ''
        
        writer = csv.DictWriter(output, fieldnames=columns)
        writer.writeheader()
        
        for row in data:
            writer.writerow(row)
        
        csv_content = output.getvalue()
        output.close()
        
        return csv_content
    
    @staticmethod
    def get_cached_result(history_id: int) -> Optional[Dict[str, Any]]:
        """
        从缓存获取查询结果
        
        Args:
            history_id: 查询历史ID
            
        Returns:
            dict: 查询结果数据，如果未缓存则返回None
        """
        try:
            history = QueryHistory.objects.get(id=history_id)
            return history.get_result_from_cache()
        except QueryHistory.DoesNotExist:
            return None
