"""
备份管理的序列化器

用于备份策略、备份记录的数据序列化和验证。
"""
from rest_framework import serializers
from apps.backups.models import BackupStrategy, BackupRecord
from apps.instances.serializers import MySQLInstanceSerializer
from apps.authentication.serializers import UserSerializer


class BackupStrategySerializer(serializers.ModelSerializer):
    """
    备份策略序列化器
    
    用于展示备份策略的详细信息。
    """
    
    instance = MySQLInstanceSerializer(read_only=True)
    created_by = UserSerializer(read_only=True)
    backup_type_display = serializers.CharField(source='get_backup_type_display', read_only=True)
    
    class Meta:
        model = BackupStrategy
        fields = [
            'id', 'name', 'instance', 'databases', 'cron_expression',
            'backup_type', 'backup_type_display', 'retention_days',
            'is_enabled', 'storage_path', 'compress', 'created_by',
            'created_at', 'updated_at'
        ]
        read_only_fields = ['created_by', 'created_at', 'updated_at']


class BackupStrategyCreateSerializer(serializers.ModelSerializer):
    """
    备份策略创建序列化器
    
    用于创建和更新备份策略。
    """
    
    instance_id = serializers.IntegerField(write_only=True, help_text='MySQL 实例 ID')
    
    class Meta:
        model = BackupStrategy
        fields = [
            'name', 'instance_id', 'databases', 'cron_expression',
            'backup_type', 'retention_days', 'is_enabled',
            'storage_path', 'compress'
        ]
    
    def validate_cron_expression(self, value):
        """
        验证 Cron 表达式格式
        
        Args:
            value: Cron 表达式
            
        Returns:
            str: 验证后的 Cron 表达式
            
        Raises:
            serializers.ValidationError: 格式错误时抛出
        """
        parts = value.strip().split()
        if len(parts) != 5:
            raise serializers.ValidationError(
                "Cron 表达式格式错误，应为 5 个字段（分 时 日 月 周），如 '0 2 * * *'"
            )
        return value

    def validate_backup_type(self, value):
        """验证备份类型"""
        valid_types = dict(BackupStrategy.BACKUP_TYPE_CHOICES).keys()
        if value not in valid_types:
            raise serializers.ValidationError("不支持的备份类型")
        return value
    
    def validate_instance_id(self, value):
        """
        验证实例 ID 是否存在
        
        Args:
            value: 实例 ID
            
        Returns:
            int: 验证后的实例 ID
            
        Raises:
            serializers.ValidationError: 实例不存在时抛出
        """
        from apps.instances.models import MySQLInstance
        
        if not MySQLInstance.objects.filter(id=value).exists():
            raise serializers.ValidationError("指定的 MySQL 实例不存在")
        return value
    
    def validate_databases(self, value):
        """
        验证数据库列表格式
        
        Args:
            value: 数据库列表
            
        Returns:
            list: 验证后的数据库列表
            
        Raises:
            serializers.ValidationError: 格式错误时抛出
        """
        if value is not None and not isinstance(value, list):
            raise serializers.ValidationError("数据库列表必须是数组格式")
        return value

    def validate(self, attrs):
        """验证策略与实例的备份配置"""
        from apps.instances.models import MySQLInstance

        instance_id = attrs.get('instance_id')
        backup_type = attrs.get('backup_type')
        if not instance_id or not backup_type:
            return attrs

        instance = MySQLInstance.objects.get(id=instance_id)

        if backup_type in ['hot', 'cold', 'incremental']:
            if not instance.data_dir:
                raise serializers.ValidationError({
                    'data_dir': '热备/冷备/增量备份必须配置实例数据目录'
                })
            if not instance.ssh_host or not instance.ssh_user:
                raise serializers.ValidationError({
                    'ssh_host': '热备/冷备/增量备份必须配置 SSH 连接信息'
                })
            if attrs.get('databases'):
                raise serializers.ValidationError({
                    'databases': '热备/冷备/增量备份不支持指定数据库列表'
                })
        if backup_type == 'cold':
            if instance.deployment_type == 'docker' and not instance.docker_container_name:
                raise serializers.ValidationError({
                    'docker_container_name': '冷备份（Docker）必须配置容器名称'
                })
            if instance.deployment_type == 'systemd' and not instance.mysql_service_name:
                raise serializers.ValidationError({
                    'mysql_service_name': '冷备份（系统服务）必须配置服务名称'
                })

        return attrs
    
    def create(self, validated_data):
        """
        创建备份策略
        
        Args:
            validated_data: 验证后的数据
            
        Returns:
            BackupStrategy: 创建的策略实例
        """
        from apps.instances.models import MySQLInstance
        
        instance_id = validated_data.pop('instance_id')
        instance = MySQLInstance.objects.get(id=instance_id)
        
        # 设置创建者
        validated_data['instance'] = instance
        validated_data['created_by'] = self.context['request'].user
        
        strategy = BackupStrategy.objects.create(**validated_data)
        
        # 如果策略启用，同步到 Celery Beat
        if strategy.is_enabled:
            from apps.backups.services import StrategyManager
            StrategyManager.sync_to_celery_beat()
        
        return strategy
    
    def update(self, instance, validated_data):
        """
        更新备份策略
        
        Args:
            instance: 策略实例
            validated_data: 验证后的数据
            
        Returns:
            BackupStrategy: 更新后的策略实例
        """
        # 如果提供了 instance_id，更新实例关联
        instance_id = validated_data.pop('instance_id', None)
        if instance_id:
            from apps.instances.models import MySQLInstance
            instance.instance = MySQLInstance.objects.get(id=instance_id)
        
        # 更新其他字段
        for attr, value in validated_data.items():
            setattr(instance, attr, value)
        
        instance.save()
        
        # 同步到 Celery Beat
        from apps.backups.services import StrategyManager
        StrategyManager.sync_to_celery_beat()
        
        return instance


class BackupRecordSerializer(serializers.ModelSerializer):
    """
    备份记录序列化器
    
    用于展示备份记录的详细信息。
    """
    
    instance = MySQLInstanceSerializer(read_only=True)
    strategy = BackupStrategySerializer(read_only=True)
    created_by = UserSerializer(read_only=True)
    status_display = serializers.CharField(source='get_status_display', read_only=True)
    backup_type_display = serializers.CharField(source='get_backup_type_display', read_only=True)
    duration_seconds = serializers.SerializerMethodField()
    download_url = serializers.SerializerMethodField()
    base_backup_id = serializers.IntegerField(read_only=True)
    
    class Meta:
        model = BackupRecord
        fields = [
            'id', 'instance', 'strategy', 'database_name', 'backup_type',
            'backup_type_display', 'status', 'status_display', 'file_path',
            'remote_path', 'object_storage_path',
            'file_size_mb', 'start_time', 'end_time', 'duration_seconds',
            'error_message', 'created_by', 'created_at', 'download_url',
            'base_backup_id'
        ]
        read_only_fields = [
            'id', 'instance', 'strategy', 'database_name', 'backup_type',
            'backup_type_display', 'status', 'status_display', 'file_path',
            'remote_path', 'object_storage_path', 'file_size_mb', 'start_time',
            'end_time', 'duration_seconds', 'error_message', 'created_by',
            'created_at', 'download_url', 'base_backup_id'
        ]
    
    def get_duration_seconds(self, obj):
        """
        获取备份耗时
        
        Args:
            obj: BackupRecord 实例
            
        Returns:
            float: 耗时秒数
        """
        return obj.get_duration_seconds()
    
    def get_download_url(self, obj):
        """
        获取下载 URL
        
        Args:
            obj: BackupRecord 实例
            
        Returns:
            str: 下载 URL
        """
        if obj.status == 'success' and obj.file_path:
            request = self.context.get('request')
            if request:
                return request.build_absolute_uri(
                    f'/api/backups/records/{obj.id}/download/'
                )
        return None


class ManualBackupSerializer(serializers.Serializer):
    """
    手动备份请求序列化器
    
    用于验证手动触发备份的请求参数。
    """
    
    backup_type = serializers.ChoiceField(
        choices=BackupRecord.BACKUP_TYPE_CHOICES,
        default='full',
        help_text='备份类型'
    )

    database_name = serializers.CharField(
        required=False,
        allow_blank=True,
        help_text='数据库名称，为空表示备份所有数据库'
    )
    
    compress = serializers.BooleanField(
        default=True,
        help_text='是否压缩备份文件'
    )

    def validate(self, attrs):
        """验证手动备份参数"""
        backup_type = attrs.get('backup_type', 'full')
        database_name = attrs.get('database_name')

        if backup_type in ['hot', 'cold', 'incremental'] and database_name:
            raise serializers.ValidationError({
                'database_name': '热备/冷备/增量备份不支持指定单个数据库'
            })
        return attrs
    
    def validate_database_name(self, value):
        """
        验证数据库名称
        
        Args:
            value: 数据库名称
            
        Returns:
            str: 验证后的数据库名称
        """
        if value:
            # 简单验证数据库名称格式（字母、数字、下划线）
            import re
            if not re.match(r'^[a-zA-Z0-9_]+$', value):
                raise serializers.ValidationError(
                    "数据库名称只能包含字母、数字和下划线"
                )
        return value


class RestoreSerializer(serializers.Serializer):
    """
    恢复请求序列化器
    
    用于验证数据恢复的请求参数，包含确认字段防止误操作。
    """
    
    target_database = serializers.CharField(
        required=False,
        allow_blank=True,
        help_text='目标数据库名称，为空则恢复到原数据库'
    )
    
    confirm = serializers.BooleanField(
        required=True,
        help_text='确认执行恢复操作（必须为 true）'
    )
    
    def validate_confirm(self, value):
        """
        验证确认字段
        
        Args:
            value: 确认值
            
        Returns:
            bool: 验证后的确认值
            
        Raises:
            serializers.ValidationError: 未确认时抛出
        """
        if not value:
            raise serializers.ValidationError(
                "恢复操作需要确认，请设置 confirm 为 true"
            )
        return value
    
    def validate_target_database(self, value):
        """
        验证目标数据库名称
        
        Args:
            value: 数据库名称
            
        Returns:
            str: 验证后的数据库名称
        """
        if value:
            # 简单验证数据库名称格式
            import re
            if not re.match(r'^[a-zA-Z0-9_]+$', value):
                raise serializers.ValidationError(
                    "数据库名称只能包含字母、数字和下划线"
                )
        return value


class RestoreUploadSerializer(serializers.Serializer):
    """
    上传备份文件恢复序列化器
    """

    instance_id = serializers.IntegerField(required=True)
    backup_file = serializers.FileField(required=True)
    target_database = serializers.CharField(
        required=False,
        allow_blank=True,
        help_text='目标数据库名称，为空则恢复到原数据库'
    )
    confirm = serializers.BooleanField(
        required=True,
        help_text='确认执行恢复操作（必须为 true）'
    )

    def validate_confirm(self, value):
        if not value:
            raise serializers.ValidationError(
                "恢复操作需要确认，请设置 confirm 为 true"
            )
        return value

    def validate_target_database(self, value):
        if value:
            import re
            if not re.match(r'^[a-zA-Z0-9_]+$', value):
                raise serializers.ValidationError(
                    "数据库名称只能包含字母、数字和下划线"
                )
        return value


class BackupRecordListSerializer(serializers.ModelSerializer):
    """
    备份记录列表序列化器
    
    用于列表展示，减少关联查询提升性能。
    """
    
    instance_alias = serializers.CharField(source='instance.alias', read_only=True)
    strategy_name = serializers.SerializerMethodField()
    status_display = serializers.CharField(source='get_status_display', read_only=True)
    backup_type_display = serializers.CharField(source='get_backup_type_display', read_only=True)
    base_backup_id = serializers.IntegerField(read_only=True)
    
    class Meta:
        model = BackupRecord
        fields = [
            'id', 'instance_alias', 'strategy_name', 'database_name',
            'backup_type', 'backup_type_display', 'status', 'status_display',
            'file_size_mb', 'start_time', 'end_time', 'created_at',
            'remote_path', 'object_storage_path',
            'base_backup_id'
        ]
        read_only_fields = [
            'id', 'instance_alias', 'strategy_name', 'database_name',
            'backup_type', 'backup_type_display', 'status', 'status_display',
            'file_size_mb', 'start_time', 'end_time', 'created_at',
            'remote_path', 'object_storage_path', 'base_backup_id'
        ]

    def get_strategy_name(self, obj):
        """防止手动备份没有策略导致序列化失败"""
        if obj.strategy_id:
            return obj.strategy.name
        return None
