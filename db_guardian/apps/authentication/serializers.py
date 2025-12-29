"""
认证应用序列化器
用于用户、角色、团队等数据的序列化和反序列化
"""
from rest_framework import serializers
from django.contrib.auth.password_validation import validate_password
from django.core.exceptions import ValidationError
from .models import User, Role, Permission, Team, TeamMember


class PermissionSerializer(serializers.ModelSerializer):
    """权限序列化器"""
    
    category_display = serializers.CharField(source='get_category_display', read_only=True)
    
    class Meta:
        model = Permission
        fields = ['id', 'name', 'slug', 'category', 'category_display', 'description', 'created_at']
        read_only_fields = ['id', 'created_at']


class RoleSerializer(serializers.ModelSerializer):
    """角色序列化器，嵌套显示权限列表"""
    
    permissions = PermissionSerializer(many=True, read_only=True)
    permission_ids = serializers.ListField(
        child=serializers.IntegerField(),
        write_only=True,
        required=False,
        help_text="权限ID列表"
    )
    permission_count = serializers.SerializerMethodField()
    
    class Meta:
        model = Role
        fields = [
            'id', 'name', 'slug', 'description', 'is_builtin',
            'permissions', 'permission_ids', 'permission_count', 'created_at'
        ]
        read_only_fields = ['id', 'is_builtin', 'created_at']
    
    def get_permission_count(self, obj):
        """获取权限数量"""
        return obj.permissions.count()
    
    def update(self, instance, validated_data):
        """更新角色，处理权限关联"""
        permission_ids = validated_data.pop('permission_ids', None)
        
        # 更新基本信息
        for attr, value in validated_data.items():
            setattr(instance, attr, value)
        instance.save()
        
        # 更新权限关联
        if permission_ids is not None:
            permissions = Permission.objects.filter(id__in=permission_ids)
            instance.permissions.set(permissions)
        
        return instance


class TeamMemberSerializer(serializers.ModelSerializer):
    """团队成员序列化器"""
    
    user_id = serializers.IntegerField(source='user.id', read_only=True)
    username = serializers.CharField(source='user.username', read_only=True)
    email = serializers.CharField(source='user.email', read_only=True)
    role_id = serializers.IntegerField(source='role.id', read_only=True)
    role_name = serializers.CharField(source='role.name', read_only=True)
    
    class Meta:
        model = TeamMember
        fields = [
            'id', 'user_id', 'username', 'email',
            'role_id', 'role_name', 'joined_at'
        ]
        read_only_fields = ['id', 'joined_at']


class TeamSerializer(serializers.ModelSerializer):
    """团队序列化器"""
    
    owner_id = serializers.IntegerField(source='owner.id', read_only=True)
    owner_username = serializers.CharField(source='owner.username', read_only=True)
    owner_email = serializers.CharField(source='owner.email', read_only=True)
    member_count = serializers.SerializerMethodField()
    
    class Meta:
        model = Team
        fields = [
            'id', 'name', 'description',
            'owner_id', 'owner_username', 'owner_email',
            'member_count', 'created_at', 'updated_at'
        ]
        read_only_fields = ['id', 'owner_id', 'created_at', 'updated_at']
    
    def get_member_count(self, obj):
        """获取成员数量"""
        return obj.members.count()


class TeamDetailSerializer(TeamSerializer):
    """团队详情序列化器，包含成员列表"""
    
    members = TeamMemberSerializer(source='memberships', many=True, read_only=True)
    
    class Meta(TeamSerializer.Meta):
        fields = TeamSerializer.Meta.fields + ['members']


class UserSerializer(serializers.ModelSerializer):
    """用户详情序列化器"""
    
    teams = serializers.SerializerMethodField()
    
    class Meta:
        model = User
        fields = [
            'id', 'username', 'email', 'phone', 'avatar',
            'is_active', 'is_staff', 'is_superuser',
            'teams', 'created_at', 'updated_at', 'last_login'
        ]
        read_only_fields = [
            'id', 'is_staff', 'is_superuser',
            'created_at', 'updated_at', 'last_login'
        ]
    
    def get_teams(self, obj):
        """获取用户所属团队信息"""
        memberships = obj.team_memberships.select_related('team', 'role')
        return [
            {
                'team_id': m.team.id,
                'team_name': m.team.name,
                'role_id': m.role.id,
                'role_name': m.role.name,
                'joined_at': m.joined_at
            }
            for m in memberships
        ]


class UserCreateSerializer(serializers.ModelSerializer):
    """用户创建序列化器"""
    
    password = serializers.CharField(
        write_only=True,
        required=True,
        style={'input_type': 'password'},
        help_text="用户初始密码，至少8位"
    )
    team_ids = serializers.ListField(
        child=serializers.IntegerField(),
        write_only=True,
        required=False,
        help_text="团队ID列表"
    )
    role_id = serializers.IntegerField(
        write_only=True,
        required=False,
        help_text="默认角色ID（用于所有团队）"
    )
    
    class Meta:
        model = User
        fields = [
            'id', 'username', 'email', 'phone', 'avatar',
            'password', 'team_ids', 'role_id',
            'is_active', 'created_at'
        ]
        read_only_fields = ['id', 'created_at']
        extra_kwargs = {
            'email': {'required': True},
        }
    
    def validate_password(self, value):
        """验证密码强度"""
        try:
            validate_password(value)
        except ValidationError as e:
            raise serializers.ValidationError(list(e.messages))
        return value
    
    def validate(self, attrs):
        """验证团队和角色"""
        team_ids = attrs.get('team_ids', [])
        role_id = attrs.get('role_id')
        
        if team_ids and not role_id:
            raise serializers.ValidationError({
                'role_id': '指定团队时必须提供角色ID'
            })
        
        if role_id:
            try:
                Role.objects.get(id=role_id)
            except Role.DoesNotExist:
                raise serializers.ValidationError({
                    'role_id': f'角色 ID {role_id} 不存在'
                })
        
        return attrs
    
    def create(self, validated_data):
        """创建用户并关联团队"""
        password = validated_data.pop('password')
        team_ids = validated_data.pop('team_ids', [])
        role_id = validated_data.pop('role_id', None)
        
        # 创建用户
        user = User.objects.create_user(
            password=password,
            **validated_data
        )
        
        # 关联团队
        if team_ids and role_id:
            role = Role.objects.get(id=role_id)
            teams = Team.objects.filter(id__in=team_ids)
            for team in teams:
                TeamMember.objects.create(
                    user=user,
                    team=team,
                    role=role
                )
        
        return user


class UserUpdateSerializer(serializers.ModelSerializer):
    """用户更新序列化器"""
    
    class Meta:
        model = User
        fields = ['email', 'phone', 'avatar', 'is_active']
    
    def update(self, instance, validated_data):
        """更新用户信息"""
        for attr, value in validated_data.items():
            setattr(instance, attr, value)
        instance.save()
        return instance


class PasswordChangeSerializer(serializers.Serializer):
    """密码修改序列化器"""
    
    old_password = serializers.CharField(
        required=True,
        write_only=True,
        style={'input_type': 'password'},
        help_text="当前密码"
    )
    new_password = serializers.CharField(
        required=True,
        write_only=True,
        style={'input_type': 'password'},
        help_text="新密码，至少8位"
    )
    
    def validate_old_password(self, value):
        """验证旧密码是否正确"""
        user = self.context['request'].user
        if not user.check_password(value):
            raise serializers.ValidationError('当前密码不正确')
        return value
    
    def validate_new_password(self, value):
        """验证新密码强度"""
        try:
            validate_password(value, user=self.context['request'].user)
        except ValidationError as e:
            raise serializers.ValidationError(list(e.messages))
        return value
    
    def validate(self, attrs):
        """验证新旧密码不能相同"""
        if attrs['old_password'] == attrs['new_password']:
            raise serializers.ValidationError({
                'new_password': '新密码不能与当前密码相同'
            })
        return attrs
    
    def save(self):
        """更新密码"""
        user = self.context['request'].user
        user.set_password(self.validated_data['new_password'])
        user.save()
        return user


class TeamMemberAddSerializer(serializers.Serializer):
    """添加团队成员序列化器"""
    
    user_id = serializers.IntegerField(required=True, help_text="用户ID")
    role_id = serializers.IntegerField(required=True, help_text="角色ID")
    
    def validate_user_id(self, value):
        """验证用户是否存在"""
        try:
            User.objects.get(id=value)
        except User.DoesNotExist:
            raise serializers.ValidationError(f'用户 ID {value} 不存在')
        return value
    
    def validate_role_id(self, value):
        """验证角色是否存在"""
        try:
            Role.objects.get(id=value)
        except Role.DoesNotExist:
            raise serializers.ValidationError(f'角色 ID {value} 不存在')
        return value
    
    def validate(self, attrs):
        """验证用户是否已是团队成员"""
        team = self.context['team']
        user_id = attrs['user_id']
        
        if TeamMember.objects.filter(team=team, user_id=user_id).exists():
            raise serializers.ValidationError({
                'user_id': '该用户已是团队成员'
            })
        
        return attrs