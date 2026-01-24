"""
SQL客户端应用验证器
提供SQL语句的安全性验证和语法检查
"""
import re
import sqlparse
from typing import Dict, List, Any, Tuple


class SQLValidator:
    """
    SQL语句验证器
    检查SQL语句的安全性和合法性，使用 sqlparse 进行语法分析
    """
    
    # 允许的查询类型（只读操作）
    ALLOWED_QUERY_TYPES = ['SELECT', 'SHOW', 'DESC', 'DESCRIBE', 'EXPLAIN']
    
    # 允许的修改类型（写操作）
    ALLOWED_MODIFY_TYPES = ['INSERT', 'UPDATE', 'DELETE']
    
    # 允许的DDL类型（结构变更）
    ALLOWED_DDL_TYPES = ['CREATE', 'ALTER', 'DROP', 'TRUNCATE', 'RENAME']
    
    # 高危操作关键字（需要特别审批）
    DANGEROUS_PATTERNS = [
        r'DROP\s+DATABASE',
        r'TRUNCATE\s+TABLE',
        r'DELETE\s+FROM\s+\w+\s+WHERE\s+1\s*=\s*1',
        r'DELETE\s+FROM\s+\w+\s*;?\s*$',  # 无 WHERE 的 DELETE
        r'UPDATE\s+\w+\s+SET.*WHERE\s+1\s*=\s*1',
        r'GRANT\s+ALL',
        r'REVOKE',
        r'SHUTDOWN',
        r'KILL',
    ]
    
    @classmethod
    def validate_sql(cls, sql: str, allowed_types: List[str] = None) -> Dict[str, Any]:
        """
        验证SQL语句的安全性和合法性
        
        Args:
            sql: SQL语句
            allowed_types: 允许的SQL类型列表，如 ['SELECT', 'INSERT']
            
        Returns:
            验证结果字典:
            {
                'is_valid': bool,           # 是否验证通过
                'sql_type': str,            # 语句类型
                'message': str,             # 验证消息
                'warnings': List[str],      # 警告信息列表
                'parsed_statements': list   # 解析后的语句列表
            }
        """
        result = {
            'is_valid': False,
            'sql_type': 'UNKNOWN',
            'message': '',
            'warnings': [],
            'parsed_statements': []
        }
        
        # 1. 基本检查
        if not sql or not sql.strip():
            result['message'] = 'SQL语句不能为空'
            return result
        
        # 2. 使用 sqlparse 解析 SQL
        try:
            parsed = sqlparse.parse(sql)
            if not parsed:
                result['message'] = 'SQL语句解析失败'
                return result
            
            result['parsed_statements'] = parsed
            
            # 获取第一条语句的类型（大多数情况下只有一条）
            if len(parsed) > 1:
                result['warnings'].append(f'检测到多条SQL语句({len(parsed)}条)，只会执行第一条')
            
            first_statement = parsed[0]
            sql_type = cls._get_statement_type(first_statement)
            result['sql_type'] = sql_type
            
        except Exception as e:
            result['message'] = f'SQL解析异常: {str(e)}'
            return result
        
        # 3. 检查SQL类型是否在允许列表中
        if allowed_types:
            # 拒绝不在允许列表中的 SQL 类型。
            if sql_type not in allowed_types:
                result['message'] = f'不允许执行 {sql_type} 类型的SQL语句'
                return result
        
        # 4. 检查危险操作
        is_dangerous, danger_msg = cls._check_dangerous_operations(sql)
        if is_dangerous:
            # 在执行前硬拦截高危模式。
            result['message'] = f'检测到危险操作: {danger_msg}'
            return result
        
        # 5. 检查SQL注入风险
        if cls._has_sql_injection_risk(sql):
            result['message'] = 'SQL语句存在注入风险'
            return result
        
        # 6. 验证通过
        result['is_valid'] = True
        result['message'] = '验证通过'
        
        return result
    
    @classmethod
    def _get_statement_type(cls, statement) -> str:
        """
        获取SQL语句类型
        
        Args:
            statement: sqlparse.sql.Statement 对象
            
        Returns:
            str: SQL类型（SELECT, INSERT, UPDATE等）
        """
        # 获取第一个有意义的token
        for token in statement.tokens:
            if token.ttype is None and isinstance(token, sqlparse.sql.Token):
                continue
            if token.ttype in (sqlparse.tokens.Keyword.DML, sqlparse.tokens.Keyword.DDL):
                return token.value.upper()
            if token.ttype is sqlparse.tokens.Keyword:
                keyword = token.value.upper()
                # 处理常见的SQL关键字
                if keyword in cls.ALLOWED_QUERY_TYPES + cls.ALLOWED_MODIFY_TYPES + cls.ALLOWED_DDL_TYPES:
                    return keyword
        
        return 'UNKNOWN'
    
    @classmethod
    def _check_dangerous_operations(cls, sql: str) -> Tuple[bool, str]:
        """
        检查是否包含危险操作
        
        Args:
            sql: SQL语句
            
        Returns:
            tuple: (是否危险, 危险描述)
        """
        sql_upper = sql.upper()
        
        for pattern in cls.DANGEROUS_PATTERNS:
            if re.search(pattern, sql_upper, re.IGNORECASE):
                return True, f'包含危险模式: {pattern}'
        
        return False, ''
    
    @staticmethod
    def _has_sql_injection_risk(sql: str) -> bool:
        """
        检查SQL注入风险
        
        Args:
            sql: SQL语句
            
        Returns:
            是否存在注入风险
        """
        # 基础注入检测模式
        injection_patterns = [
            r';\s*DROP\s+',
            r';\s*DELETE\s+FROM',
            r'UNION\s+ALL\s+SELECT',
            r'UNION\s+SELECT',
            r'/\*.*\*/',  # 多行注释
            r'--\s*\S',   # 注释后跟内容（可能是注入）
            r';\s*EXEC\s*\(',
            r';\s*EXECUTE\s*\(',
            r'xp_cmdshell',
            r'sp_executesql',
        ]
        
        for pattern in injection_patterns:
            if re.search(pattern, sql, re.IGNORECASE):
                return True
        
        return False
    
    @classmethod
    def apply_row_limit(cls, sql: str, limit: int = 1000) -> str:
        """
        为SELECT查询添加行数限制
        
        如果SQL已有LIMIT子句，取两者中的最小值
        
        Args:
            sql: SQL语句
            limit: 最大行数限制
            
        Returns:
            str: 添加了LIMIT的SQL语句
        """
        # 解析SQL
        parsed = sqlparse.parse(sql)
        if not parsed:
            return sql
        
        statement = parsed[0]
        sql_type = cls._get_statement_type(statement)
        
        # 只对SELECT查询添加限制
        if sql_type != 'SELECT':
            return sql
        
        # 检查是否已有LIMIT
        sql_upper = sql.upper()
        # 检测已有 LIMIT，保留更严格的限制。
        limit_match = re.search(r'LIMIT\s+(\d+)', sql_upper)
        
        if limit_match:
            # 已有LIMIT，取最小值
            existing_limit = int(limit_match.group(1))
            if existing_limit <= limit:
                return sql  # 现有限制更严格，不修改
            
            # 替换为更严格的限制
            return re.sub(r'LIMIT\s+\d+', f'LIMIT {limit}', sql, flags=re.IGNORECASE)
        
        # 添加LIMIT
        # 移除末尾的分号（如果有）
        sql = sql.rstrip().rstrip(';')
        return f"{sql} LIMIT {limit}"
    
    @classmethod
    def is_safe_sql(cls, sql: str) -> bool:
        """
        快速检查SQL是否安全（不做完整验证）
        
        Args:
            sql: SQL语句
            
        Returns:
            bool: 是否安全
        """
        # 检查危险操作
        is_dangerous, _ = cls._check_dangerous_operations(sql)
        if is_dangerous:
            return False
        
        # 检查注入风险
        if cls._has_sql_injection_risk(sql):
            return False
        
        return True
    
    @staticmethod
    def parse_sql_type(sql: str) -> str:
        """
        解析SQL语句类型（简单版本，用于向后兼容）
        
        Args:
            sql: SQL语句
            
        Returns:
            SQL类型 (SELECT, INSERT, UPDATE, DELETE等)
        """
        sql = sql.strip().upper()
        
        # 跳过注释和空白
        sql = re.sub(r'/\*.*?\*/', '', sql, flags=re.DOTALL)
        sql = re.sub(r'--.*?$', '', sql, flags=re.MULTILINE)
        sql = sql.strip()
        
        # 检查常见的SQL类型
        if sql.startswith('SELECT'):
            return 'SELECT'
        elif sql.startswith('INSERT'):
            return 'INSERT'
        elif sql.startswith('UPDATE'):
            return 'UPDATE'
        elif sql.startswith('DELETE'):
            return 'DELETE'
        elif sql.startswith('SHOW'):
            return 'SHOW'
        elif sql.startswith('DESC') or sql.startswith('DESCRIBE'):
            return 'DESC'
        elif sql.startswith('EXPLAIN'):
            return 'EXPLAIN'
        elif sql.startswith(('CREATE', 'ALTER', 'DROP', 'TRUNCATE')):
            return 'DDL'
        else:
            return 'OTHER'
