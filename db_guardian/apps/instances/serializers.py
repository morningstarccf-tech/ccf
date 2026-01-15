"""
MySQL 实例管理的序列化器

用于 MySQL 实例、数据库、监控指标等数据的序列化和反序列化。
"""
from rest_framework import serializers
from apps.instances.models import MySQLInstance, Database, MonitoringMetrics
from apps.authentication.models import Team


class MySQLInstanceSerializer(serializers.ModelSerializer):
    """
    MySQL 实例序列化器（详情和列表）
    
    用于返回实例信息，密码字段不包含在响应中。
    """
    
    team_name = serializers.CharField(source='team.name', read_only=True)
    created_by_username = serializers.CharField(source='created_by.username', read_only=True)
    status_display = serializers.CharField(source='get_status_display', read_only=True)
    database_count = serializers.SerializerMethodField()
    
    class Meta:
        model = MySQLInstance
        fields = [
            'id', 'alias', 'host', 'port', 'username',
            'team', 'team_name', 'description', 'status', 'status_display',
            'last_check_time', 'version', 'charset',
            'deployment_type', 'docker_container_name', 'mysql_service_name',
            'data_dir', 'ssh_host', 'ssh_port', 'ssh_user', 'ssh_key_path',
            'xtrabackup_bin',
            'created_by', 'created_by_username',
            'created_at', 'updated_at', 'database_count'
        ]
        read_only_fields = [
            'id', 'status', 'last_check_time', 'version',
            'created_by', 'created_at', 'updated_at'
        ]
    
    def get_database_count(self, obj):
        """获取数据库数量"""
        return obj.databases.count()


class MySQLInstanceCreateSerializer(serializers.ModelSerializer):
    """
    MySQL 实例创建序列化器
    
    接收明文密码，在保存时自动加密。
    """
    
    password = serializers.CharField(
        write_only=True,
        style={'input_type': 'password'},
        help_text='MySQL 连接密码（将被加密存储）'
    )
    ssh_password = serializers.CharField(
        write_only=True,
        required=False,
        allow_blank=True,
        style={'input_type': 'password'},
        help_text='SSH 密码（将被加密存储）'
    )
    
    class Meta:
        model = MySQLInstance
        fields = [
            'alias', 'host', 'port', 'username', 'password',
            'team', 'description', 'charset',
            'deployment_type', 'docker_container_name', 'mysql_service_name',
            'data_dir', 'ssh_host', 'ssh_port', 'ssh_user', 'ssh_password',
            'ssh_key_path', 'xtrabackup_bin'
        ]
    
    def validate_alias(self, value):
        """验证别名唯一性"""
        if MySQLInstance.objects.filter(alias=value).exists():
            raise serializers.ValidationError("该别名已存在，请使用其他别名")
        return value
    
    def validate_port(self, value):
        """验证端口号范围"""
        if not (1 <= value <= 65535):
            raise serializers.ValidationError("端口号必须在 1-65535 之间")
        return value
    
    def validate(self, attrs):
        """验证团队访问权限"""
        request = self.context.get('request')
        if request and request.user:
            team = attrs.get('team')
            # 检查用户是否有权限在该团队中创建实例
            if not request.user.is_superuser:
                if not team.members.filter(id=request.user.id).exists():
                    raise serializers.ValidationError("您不是该团队的成员，无法创建实例")
        deployment_type = attrs.get('deployment_type')
        if deployment_type == 'docker' and not attrs.get('docker_container_name'):
            raise serializers.ValidationError({
                'docker_container_name': 'Docker 部署方式必须填写容器名称'
            })
        if deployment_type == 'systemd' and not attrs.get('mysql_service_name'):
            raise serializers.ValidationError({
                'mysql_service_name': '系统服务部署必须填写服务名称'
            })
        ssh_host = attrs.get('ssh_host')
        ssh_user = attrs.get('ssh_user')
        if ssh_host and not ssh_user:
            raise serializers.ValidationError({
                'ssh_user': '配置 SSH 主机时必须填写 SSH 用户'
            })
        return attrs
    
    def create(self, validated_data):
        """创建实例，自动设置创建者"""
        request = self.context.get('request')
        if request and request.user:
            validated_data['created_by'] = request.user
        return super().create(validated_data)


class MySQLInstanceUpdateSerializer(serializers.ModelSerializer):
    """
    MySQL 实例更新序列化器
    
    允许更新连接信息和配置，密码为可选字段。
    """
    
    password = serializers.CharField(
        write_only=True,
        required=False,
        allow_blank=True,
        style={'input_type': 'password'},
        help_text='MySQL 连接密码（留空则不修改）'
    )
    ssh_password = serializers.CharField(
        write_only=True,
        required=False,
        allow_blank=True,
        style={'input_type': 'password'},
        help_text='SSH 密码（留空则不修改）'
    )
    
    class Meta:
        model = MySQLInstance
        fields = [
            'alias', 'host', 'port', 'username', 'password',
            'description', 'charset',
            'deployment_type', 'docker_container_name', 'mysql_service_name',
            'data_dir', 'ssh_host', 'ssh_port', 'ssh_user', 'ssh_password',
            'ssh_key_path', 'xtrabackup_bin'
        ]
    
    def validate_alias(self, value):
        """验证别名唯一性（排除自身）"""
        instance = self.instance
        if MySQLInstance.objects.filter(alias=value).exclude(pk=instance.pk).exists():
            raise serializers.ValidationError("该别名已存在，请使用其他别名")
        return value
    
    def update(self, instance, validated_data):
        """更新实例，如果密码为空则不更新密码"""
        password = validated_data.pop('password', None)
        ssh_password = validated_data.pop('ssh_password', None)
        
        # 更新其他字段
        for attr, value in validated_data.items():
            setattr(instance, attr, value)
        
        # 如果提供了新密码，则更新密码
        if password:
            instance.password = password
        if ssh_password:
            instance.ssh_password = ssh_password
        
        instance.save()
        return instance


class ConnectionTestSerializer(serializers.Serializer):
    """
    连接测试序列化器
    
    用于测试 MySQL 连接的请求和响应。
    """
    
    host = serializers.CharField(required=True, help_text='主机地址')
    port = serializers.IntegerField(required=True, help_text='端口号')
    username = serializers.CharField(required=True, help_text='用户名')
    password = serializers.CharField(
        required=True,
        write_only=True,
        style={'input_type': 'password'},
        help_text='密码'
    )
    
    # 响应字段
    success = serializers.BooleanField(read_only=True, help_text='是否连接成功')
    message = serializers.CharField(read_only=True, help_text='测试结果消息')
    version = serializers.CharField(read_only=True, required=False, help_text='MySQL 版本')
    charset = serializers.CharField(read_only=True, required=False, help_text='字符集')
    
    def validate_port(self, value):
        """验证端口号"""
        if not (1 <= value <= 65535):
            raise serializers.ValidationError("端口号必须在 1-65535 之间")
        return value


class DatabaseSerializer(serializers.ModelSerializer):
    """
    数据库序列化器
    
    用于管理 MySQL 实例中的数据库。
    """
    
    instance_alias = serializers.CharField(source='instance.alias', read_only=True)
    size_mb_display = serializers.SerializerMethodField()
    
    class Meta:
        model = Database
        fields = [
            'id', 'instance', 'instance_alias', 'name',
            'charset', 'collation', 'size_mb', 'size_mb_display',
            'table_count', 'last_backup_time',
            'created_at', 'updated_at'
        ]
        read_only_fields = [
            'id', 'size_mb', 'table_count', 'last_backup_time',
            'created_at', 'updated_at'
        ]
    
    def get_size_mb_display(self, obj):
        """格式化显示数据库大小"""
        if obj.size_mb < 1024:
            return f"{obj.size_mb:.2f} MB"
        else:
            return f"{obj.size_mb / 1024:.2f} GB"
    
    def validate(self, attrs):
        """验证数据库名称在实例中的唯一性"""
        instance = attrs.get('instance')
        name = attrs.get('name')
        
        # 更新时排除自身
        queryset = Database.objects.filter(instance=instance, name=name)
        if self.instance:
            queryset = queryset.exclude(pk=self.instance.pk)
        
        if queryset.exists():
            raise serializers.ValidationError({
                'name': f"数据库 '{name}' 在该实例中已存在"
            })
        
        return attrs


class MonitoringMetricsSerializer(serializers.ModelSerializer):
    """
    监控指标序列化器
    
    用于展示实例的性能监控数据。
    """
    
    instance_alias = serializers.CharField(source='instance.alias', read_only=True)
    timestamp_display = serializers.DateTimeField(
        source='timestamp',
        format='%Y-%m-%d %H:%M:%S',
        read_only=True
    )
    
    class Meta:
        model = MonitoringMetrics
        fields = [
            'id', 'instance', 'instance_alias', 'timestamp', 'timestamp_display',
            'qps', 'tps', 'connections', 'slow_queries',
            'cpu_usage', 'memory_usage', 'disk_usage'
        ]
        read_only_fields = ['id', 'timestamp']


class DashboardSerializer(serializers.Serializer):
    """
    仪表盘数据序列化器
    
    聚合实例的各类统计信息和监控数据。
    """
    
    # 基本信息
    instance_id = serializers.IntegerField(read_only=True)
    alias = serializers.CharField(read_only=True)
    status = serializers.CharField(read_only=True)
    version = serializers.CharField(read_only=True)
    
    # 数据库统计
    database_count = serializers.IntegerField(read_only=True)
    total_size_mb = serializers.FloatField(read_only=True)
    
    # 最新监控指标
    current_metrics = MonitoringMetricsSerializer(read_only=True, required=False)
    
    # 历史监控数据（最近24小时）
    metrics_history = MonitoringMetricsSerializer(many=True, read_only=True, required=False)
    
    # 连接信息
    connection_info = serializers.DictField(read_only=True, required=False)
