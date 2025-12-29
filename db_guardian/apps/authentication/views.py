"""
认证应用视图
实现用户、团队、角色、权限的 API 接口
"""
from rest_framework import viewsets, status
from rest_framework.decorators import action
from rest_framework.response import Response
from rest_framework.permissions import IsAuthenticated, AllowAny
from rest_framework_simplejwt.views import TokenObtainPairView
from django_filters.rest_framework import DjangoFilterBackend
from rest_framework import filters

from .models import User, Role, Permission, Team, TeamMember
from .serializers import (
    UserSerializer, UserCreateSerializer, UserUpdateSerializer,
    PasswordChangeSerializer, RoleSerializer, PermissionSerializer,
    TeamSerializer, TeamDetailSerializer, TeamMemberSerializer,
    TeamMemberAddSerializer
)
from .permissions import IsSuperAdmin, IsTeamOwner, HasTeamPermission


class CustomTokenObtainPairView(TokenObtainPairView):
    """
    自定义登录视图，扩展返回用户信息
    """
    
    def post(self, request, *args, **kwargs):
        """登录并返回 token 和用户信息"""
        response = super().post(request, *args, **kwargs)
        
        if response.status_code == 200:
            # 获取用户信息
            username = request.data.get('username')
            try:
                user = User.objects.get(username=username)
                user_data = UserSerializer(user).data
                response.data['user'] = user_data
            except User.DoesNotExist:
                pass
        
        return response


class UserViewSet(viewsets.ModelViewSet):
    """
    用户管理 ViewSet
    
    提供用户的 CRUD 操作以及密码修改等功能
    """
    
    queryset = User.objects.all().order_by('-created_at')
    serializer_class = UserSerializer
    permission_classes = [IsAuthenticated]
    filter_backends = [DjangoFilterBackend, filters.SearchFilter, filters.OrderingFilter]
    filterset_fields = ['is_active', 'is_staff', 'is_superuser']
    search_fields = ['username', 'email', 'phone']
    ordering_fields = ['created_at', 'updated_at', 'username']
    
    def get_serializer_class(self):
        """根据操作类型选择序列化器"""
        if self.action == 'create':
            return UserCreateSerializer
        elif self.action in ['update', 'partial_update']:
            return UserUpdateSerializer
        return UserSerializer
    
    def get_permissions(self):
        """根据操作类型设置权限"""
        if self.action in ['create', 'destroy']:
            # 只有超级管理员可以创建和删除用户
            permission_classes = [IsAuthenticated, IsSuperAdmin]
        elif self.action == 'me':
            # 获取当前用户信息不需要额外权限
            permission_classes = [IsAuthenticated]
        else:
            permission_classes = [IsAuthenticated]
        return [permission() for permission in permission_classes]
    
    def list(self, request, *args, **kwargs):
        """
        获取用户列表
        
        支持通过 username、email、phone 搜索
        支持按 is_active、is_staff、is_superuser 过滤
        """
        return super().list(request, *args, **kwargs)
    
    def retrieve(self, request, *args, **kwargs):
        """获取用户详情"""
        return super().retrieve(request, *args, **kwargs)
    
    def create(self, request, *args, **kwargs):
        """
        创建用户
        
        只有超级管理员可以创建用户
        可以同时指定用户所属团队和角色
        """
        return super().create(request, *args, **kwargs)
    
    def update(self, request, *args, **kwargs):
        """更新用户信息"""
        # 普通用户只能更新自己的信息
        if not request.user.is_superuser:
            if int(kwargs.get('pk')) != request.user.id:
                return Response(
                    {'detail': '您只能修改自己的信息'},
                    status=status.HTTP_403_FORBIDDEN
                )
        return super().update(request, *args, **kwargs)
    
    def partial_update(self, request, *args, **kwargs):
        """部分更新用户信息"""
        return self.update(request, *args, **kwargs)
    
    def destroy(self, request, *args, **kwargs):
        """
        删除用户
        
        只有超级管理员可以删除用户
        不能删除自己
        """
        user = self.get_object()
        if user.id == request.user.id:
            return Response(
                {'detail': '不能删除自己的账户'},
                status=status.HTTP_400_BAD_REQUEST
            )
        return super().destroy(request, *args, **kwargs)
    
    @action(detail=False, methods=['get'])
    def me(self, request):
        """
        获取当前登录用户信息
        
        GET /api/users/me/
        """
        serializer = self.get_serializer(request.user)
        return Response(serializer.data)
    
    @action(detail=False, methods=['post'])
    def change_password(self, request):
        """
        修改当前用户密码
        
        POST /api/users/change_password/
        {
            "old_password": "旧密码",
            "new_password": "新密码"
        }
        """
        serializer = PasswordChangeSerializer(
            data=request.data,
            context={'request': request}
        )
        serializer.is_valid(raise_exception=True)
        serializer.save()
        
        return Response(
            {'detail': '密码修改成功'},
            status=status.HTTP_200_OK
        )


class RoleViewSet(viewsets.ReadOnlyModelViewSet):
    """
    角色管理 ViewSet
    
    提供角色的查询功能（只读）
    超级管理员可以通过 Django Admin 管理角色
    """
    
    queryset = Role.objects.all().order_by('-is_builtin', 'slug')
    serializer_class = RoleSerializer
    permission_classes = [IsAuthenticated]
    filter_backends = [DjangoFilterBackend, filters.SearchFilter]
    filterset_fields = ['is_builtin']
    search_fields = ['name', 'slug', 'description']
    
    def list(self, request, *args, **kwargs):
        """获取角色列表"""
        return super().list(request, *args, **kwargs)
    
    def retrieve(self, request, *args, **kwargs):
        """获取角色详情，包含权限列表"""
        return super().retrieve(request, *args, **kwargs)


class PermissionViewSet(viewsets.ReadOnlyModelViewSet):
    """
    权限管理 ViewSet
    
    提供权限的查询功能（只读）
    """
    
    queryset = Permission.objects.all().order_by('category', 'slug')
    serializer_class = PermissionSerializer
    permission_classes = [IsAuthenticated]
    filter_backends = [DjangoFilterBackend, filters.SearchFilter]
    filterset_fields = ['category']
    search_fields = ['name', 'slug', 'description']
    
    def list(self, request, *args, **kwargs):
        """获取权限列表"""
        return super().list(request, *args, **kwargs)
    
    def retrieve(self, request, *args, **kwargs):
        """获取权限详情"""
        return super().retrieve(request, *args, **kwargs)
    
    @action(detail=False, methods=['get'])
    def by_category(self, request):
        """
        按分类获取权限
        
        GET /api/permissions/by_category/
        
        返回格式：
        {
            "user": [...],
            "team": [...],
            "instance": [...],
            ...
        }
        """
        permissions = self.get_queryset()
        result = {}
        
        for perm in permissions:
            if perm.category not in result:
                result[perm.category] = []
            result[perm.category].append(
                PermissionSerializer(perm).data
            )
        
        return Response(result)


class TeamViewSet(viewsets.ModelViewSet):
    """
    团队管理 ViewSet
    
    提供团队的 CRUD 操作以及成员管理功能
    """
    
    queryset = Team.objects.all().order_by('-created_at')
    serializer_class = TeamSerializer
    permission_classes = [IsAuthenticated]
    filter_backends = [DjangoFilterBackend, filters.SearchFilter, filters.OrderingFilter]
    search_fields = ['name', 'description']
    ordering_fields = ['created_at', 'updated_at', 'name']
    
    def get_serializer_class(self):
        """根据操作类型选择序列化器"""
        if self.action == 'retrieve':
            return TeamDetailSerializer
        return TeamSerializer
    
    def get_permissions(self):
        """根据操作类型设置权限"""
        if self.action in ['create']:
            # 任何认证用户都可以创建团队
            permission_classes = [IsAuthenticated]
        elif self.action in ['update', 'partial_update', 'destroy']:
            # 只有超级管理员和团队所有者可以修改和删除团队
            permission_classes = [IsAuthenticated, IsTeamOwner]
        else:
            permission_classes = [IsAuthenticated]
        return [permission() for permission in permission_classes]
    
    def get_queryset(self):
        """
        过滤团队列表
        
        超级管理员可以看到所有团队
        普通用户只能看到自己所属的团队
        """
        user = self.request.user
        if user.is_superuser:
            return Team.objects.all()
        return Team.objects.filter(members=user).distinct()
    
    def create(self, request, *args, **kwargs):
        """
        创建团队
        
        创建者自动成为团队所有者
        """
        serializer = self.get_serializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        
        # 设置创建者为所有者
        team = serializer.save(owner=request.user)
        
        # 将创建者添加为团队成员（超级管理员角色）
        try:
            super_admin_role = Role.objects.get(slug='super_admin')
            TeamMember.objects.create(
                team=team,
                user=request.user,
                role=super_admin_role
            )
        except Role.DoesNotExist:
            # 如果没有超级管理员角色，使用第一个可用角色
            first_role = Role.objects.first()
            if first_role:
                TeamMember.objects.create(
                    team=team,
                    user=request.user,
                    role=first_role
                )
        
        headers = self.get_success_headers(serializer.data)
        return Response(
            TeamDetailSerializer(team).data,
            status=status.HTTP_201_CREATED,
            headers=headers
        )
    
    @action(detail=True, methods=['get'])
    def members(self, request, pk=None):
        """
        获取团队成员列表
        
        GET /api/teams/{id}/members/
        """
        team = self.get_object()
        members = team.memberships.select_related('user', 'role').all()
        serializer = TeamMemberSerializer(members, many=True)
        return Response(serializer.data)
    
    @action(detail=True, methods=['post'])
    def add_member(self, request, pk=None):
        """
        添加团队成员
        
        POST /api/teams/{id}/add_member/
        {
            "user_id": 1,
            "role_id": 2
        }
        
        需要团队所有者权限或超级管理员权限
        """
        team = self.get_object()
        
        # 权限检查
        if not (request.user.is_superuser or team.owner.id == request.user.id):
            return Response(
                {'detail': '只有团队所有者或超级管理员可以添加成员'},
                status=status.HTTP_403_FORBIDDEN
            )
        
        serializer = TeamMemberAddSerializer(
            data=request.data,
            context={'team': team}
        )
        serializer.is_valid(raise_exception=True)
        
        # 添加成员
        user = User.objects.get(id=serializer.validated_data['user_id'])
        role = Role.objects.get(id=serializer.validated_data['role_id'])
        
        member = TeamMember.objects.create(
            team=team,
            user=user,
            role=role
        )
        
        return Response(
            TeamMemberSerializer(member).data,
            status=status.HTTP_201_CREATED
        )
    
    @action(detail=True, methods=['post'])
    def remove_member(self, request, pk=None):
        """
        移除团队成员
        
        POST /api/teams/{id}/remove_member/
        {
            "user_id": 1
        }
        
        需要团队所有者权限或超级管理员权限
        不能移除团队所有者
        """
        team = self.get_object()
        
        # 权限检查
        if not (request.user.is_superuser or team.owner.id == request.user.id):
            return Response(
                {'detail': '只有团队所有者或超级管理员可以移除成员'},
                status=status.HTTP_403_FORBIDDEN
            )
        
        user_id = request.data.get('user_id')
        if not user_id:
            return Response(
                {'detail': '缺少 user_id 参数'},
                status=status.HTTP_400_BAD_REQUEST
            )
        
        # 不能移除团队所有者
        if team.owner.id == user_id:
            return Response(
                {'detail': '不能移除团队所有者'},
                status=status.HTTP_400_BAD_REQUEST
            )
        
        # 删除成员关系
        deleted_count, _ = TeamMember.objects.filter(
            team=team,
            user_id=user_id
        ).delete()
        
        if deleted_count == 0:
            return Response(
                {'detail': '该用户不是团队成员'},
                status=status.HTTP_404_NOT_FOUND
            )
        
        return Response(
            {'detail': '成员已移除'},
            status=status.HTTP_200_OK
        )
    
    @action(detail=True, methods=['post'])
    def update_member_role(self, request, pk=None):
        """
        更新团队成员角色
        
        POST /api/teams/{id}/update_member_role/
        {
            "user_id": 1,
            "role_id": 2
        }
        
        需要团队所有者权限或超级管理员权限
        """
        team = self.get_object()
        
        # 权限检查
        if not (request.user.is_superuser or team.owner.id == request.user.id):
            return Response(
                {'detail': '只有团队所有者或超级管理员可以更新成员角色'},
                status=status.HTTP_403_FORBIDDEN
            )
        
        user_id = request.data.get('user_id')
        role_id = request.data.get('role_id')
        
        if not user_id or not role_id:
            return Response(
                {'detail': '缺少 user_id 或 role_id 参数'},
                status=status.HTTP_400_BAD_REQUEST
            )
        
        # 获取成员关系
        try:
            member = TeamMember.objects.get(team=team, user_id=user_id)
        except TeamMember.DoesNotExist:
            return Response(
                {'detail': '该用户不是团队成员'},
                status=status.HTTP_404_NOT_FOUND
            )
        
        # 获取角色
        try:
            role = Role.objects.get(id=role_id)
        except Role.DoesNotExist:
            return Response(
                {'detail': f'角色 ID {role_id} 不存在'},
                status=status.HTTP_404_NOT_FOUND
            )
        
        # 更新角色
        member.role = role
        member.save()
        
        return Response(
            TeamMemberSerializer(member).data,
            status=status.HTTP_200_OK
        )
