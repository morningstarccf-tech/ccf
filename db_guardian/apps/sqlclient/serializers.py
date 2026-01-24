"""
SQL客户端应用序列化器
用于SQL执行历史等数据的序列化和反序列化
"""
from rest_framework import serializers
from django.conf import settings

from .models import QueryHistory


class QueryExecutionSerializer(serializers.Serializer):
    """
    SQL执行请求序列化器
    
    用于验证和处理SQL执行的输入参数
    """
    
    sql = serializers.CharField(
        required=True,
        allow_blank=False,
        help_text='要执行的SQL语句',
        error_messages={
            'required': 'SQL语句不能为空',
            'blank': 'SQL语句不能为空'
        }
    )
    
    database = serializers.CharField(
        required=False,
        allow_blank=True,
        max_length=100,
        help_text='目标数据库名称（可选）'
    )
    
    timeout = serializers.IntegerField(
        required=False,
        default=30,
        min_value=1,
        max_value=300,
        help_text='查询超时时间（秒），范围 1-300'
    )
    
    apply_limit = serializers.BooleanField(
        required=False,
        default=True,
        help_text='是否自动应用行数限制（仅SELECT查询）'
    )
    
    max_rows = serializers.IntegerField(
        required=False,
        default=1000,
        min_value=1,
        max_value=10000,
        help_text='最大返回行数，范围 1-10000'
    )
    
    def validate_sql(self, value):
        """验证SQL不为空且不是纯空白"""
        if not value.strip():
            raise serializers.ValidationError('SQL语句不能为空')
        return value.strip()
    
    def validate_timeout(self, value):
        """验证超时时间在合理范围内"""
        max_timeout = getattr(settings, 'SQL_QUERY_TIMEOUT', 30)
        if value > max_timeout:
            raise serializers.ValidationError(f'超时时间不能超过 {max_timeout} 秒')
        return value


class QueryResultSerializer(serializers.Serializer):
    """
    查询结果序列化器
    
    用于返回SQL执行结果
    """
    
    success = serializers.BooleanField(help_text='执行是否成功')
    sql_type = serializers.CharField(help_text='SQL类型')
    rows_affected = serializers.IntegerField(help_text='影响或返回的行数')
    execution_time_ms = serializers.IntegerField(help_text='执行时间（毫秒）')
    columns = serializers.ListField(
        child=serializers.CharField(),
        required=False,
        help_text='结果集的列名列表'
    )
    data = serializers.ListField(
        child=serializers.DictField(),
        required=False,
        help_text='结果集数据'
    )
    message = serializers.CharField(help_text='执行消息')
    history_id = serializers.IntegerField(
        required=False,
        allow_null=True,
        help_text='执行历史记录ID'
    )
    warnings = serializers.ListField(
        child=serializers.CharField(),
        required=False,
        help_text='警告信息列表'
    )


class QueryHistorySerializer(serializers.ModelSerializer):
    """
    SQL执行历史序列化器
    
    用于展示SQL执行历史记录
    """
    
    instance_alias = serializers.CharField(
        source='instance.alias',
        read_only=True,
        help_text='实例别名'
    )
    
    executed_by_username = serializers.CharField(
        source='executed_by.username',
        read_only=True,
        help_text='执行者用户名'
    )
    
    status_display = serializers.CharField(
        source='get_status_display',
        read_only=True,
        help_text='状态显示名称'
    )
    
    sql_type_display = serializers.CharField(
        source='get_sql_type_display',
        read_only=True,
        help_text='SQL类型显示名称'
    )
    
    class Meta:
        model = QueryHistory
        fields = [
            'id',
            'instance',
            'instance_alias',
            'database_name',
            'sql_statement',
            'sql_type',
            'sql_type_display',
            'status',
            'status_display',
            'rows_affected',
            'execution_time_ms',
            'error_message',
            'result_cached',
            'executed_by',
            'executed_by_username',
            'executed_at'
        ]
        read_only_fields = ['id', 'executed_at']


class QueryHistoryDetailSerializer(QueryHistorySerializer):
    """
    SQL执行历史详情序列化器
    
    包含更详细的信息，如完整的SQL语句
    """
    
    instance_info = serializers.SerializerMethodField(help_text='实例详细信息')
    
    class Meta(QueryHistorySerializer.Meta):
        fields = QueryHistorySerializer.Meta.fields + ['instance_info']
    
    def get_instance_info(self, obj):
        """获取实例详细信息"""
        return {
            'id': obj.instance.id,
            'alias': obj.instance.alias,
            'host': obj.instance.host,
            'port': obj.instance.port,
            'status': obj.instance.status
        }


class ColumnSerializer(serializers.Serializer):
    """列信息序列化器"""
    name = serializers.CharField()
    type = serializers.CharField()
    null = serializers.CharField()
    key = serializers.CharField()
    default = serializers.CharField(allow_null=True)
    extra = serializers.CharField()
    comment = serializers.CharField(allow_blank=True)


class IndexSerializer(serializers.Serializer):
    """索引信息序列化器"""
    name = serializers.CharField()
    unique = serializers.BooleanField()
    columns = serializers.ListField(child=serializers.CharField())


class TableSerializer(serializers.Serializer):
    """表信息序列化器"""
    name = serializers.CharField()
    type = serializers.CharField()
    engine = serializers.CharField()
    rows = serializers.IntegerField()
    columns = ColumnSerializer(many=True)
    indexes = IndexSerializer(many=True)


class DatabaseSerializer(serializers.Serializer):
    """数据库信息序列化器"""
    name = serializers.CharField()
    tables = TableSerializer(many=True)


class SchemaSerializer(serializers.Serializer):
    """
    数据库结构序列化器
    
    用于返回数据库的树状结构
    """
    databases = DatabaseSerializer(many=True)


class ExportRequestSerializer(serializers.Serializer):
    """
    导出请求序列化器
    
    用于验证导出请求参数
    """
    
    FORMAT_CHOICES = [
        ('csv', 'CSV'),
    ]
    
    format = serializers.ChoiceField(
        choices=FORMAT_CHOICES,
        default='csv',
        help_text='导出格式'
    )
    
    history_id = serializers.IntegerField(
        required=True,
        help_text='查询历史记录ID'
    )
    
    def validate_history_id(self, value):
        """验证历史记录是否存在"""
        try:
            history = QueryHistory.objects.get(id=value)
            # 检查是否有缓存的结果
            if not history.result_cached:
                raise serializers.ValidationError('该查询结果未缓存，无法导出')
            # 这里只校验存在性和缓存状态，权限由视图层校验。
            return value
        except QueryHistory.DoesNotExist:
            raise serializers.ValidationError('查询历史记录不存在')
