"""
认证应用权限类
定义自定义权限规则，用于 API 访问控制
"""
from rest_framework import permissions


class IsSuperAdmin(permissions.BasePermission):
    """
    验证用户是否为超级管理员
    
    超级管理员拥有所有权限
    """
    message = '只有超级管理员可以执行此操作'
    
    def has_permission(self, request, view):
        """检查用户是否为超级管理员"""
        return request.user and request.user.is_authenticated and request.user.is_superuser
    
    def has_object_permission(self, request, view, obj):
        """对象级别权限检查"""
        return request.user and request.user.is_superuser


class IsTeamOwner(permissions.BasePermission):
    """
    验证用户是否为团队所有者
    
    用于团队的修改、删除等操作
    超级管理员也拥有该权限
    """
    message = '只有团队所有者或超级管理员可以执行此操作'
    
    def has_permission(self, request, view):
        """基本权限检查"""
        return request.user and request.user.is_authenticated
    
    def has_object_permission(self, request, view, obj):
        """
        对象级别权限检查
        
        obj 可能是 Team 实例
        """
        # 超级管理员拥有所有权限
        if request.user.is_superuser:
            return True
        
        # 检查是否为团队所有者
        # 如果 obj 是 Team 实例
        if hasattr(obj, 'owner'):
            return obj.owner.id == request.user.id
        
        return False


class IsTeamMember(permissions.BasePermission):
    """
    验证用户是否为指定团队成员
    
    用于检查用户是否有权访问团队资源
    """
    message = '您不是该团队的成员'
    
    def has_permission(self, request, view):
        """基本权限检查"""
        return request.user and request.user.is_authenticated
    
    def has_object_permission(self, request, view, obj):
        """
        对象级别权限检查
        
        obj 可能是 Team 实例或其他关联团队的对象
        """
        # 超级管理员拥有所有权限
        if request.user.is_superuser:
            return True
        
        # 获取团队对象
        team = None
        if hasattr(obj, 'team'):
            # 如果对象有 team 属性（如 Instance, Backup 等）
            team = obj.team
        elif hasattr(obj, 'members'):
            # 如果对象本身就是 Team
            team = obj
        
        if team:
            # 检查用户是否为团队成员
            return team.members.filter(id=request.user.id).exists()
        
        return False


class IsTeamAdmin(permissions.BasePermission):
    """
    验证用户是否为团队管理员
    
    团队管理员包括：
    1. 团队所有者
    2. 拥有 team_admin 角色的成员
    3. 超级管理员
    """
    message = '您不是该团队的管理员'
    
    def has_permission(self, request, view):
        """基本权限检查"""
        return request.user and request.user.is_authenticated
    
    def has_object_permission(self, request, view, obj):
        """对象级别权限检查"""
        # 超级管理员拥有所有权限
        if request.user.is_superuser:
            return True
        
        # 获取团队对象
        team = None
        if hasattr(obj, 'team'):
            team = obj.team
        elif hasattr(obj, 'members'):
            team = obj
        
        if not team:
            return False
        
        # 检查是否为团队所有者
        if team.owner.id == request.user.id:
            return True
        
        # 检查是否拥有管理员角色
        try:
            from .models import TeamMember
            membership = TeamMember.objects.get(
                team=team,
                user=request.user
            )
            # 检查角色是否有管理权限
            # 这里可以根据实际需求检查特定权限
            return membership.role.has_permission('manage_team')
        except TeamMember.DoesNotExist:
            return False


class HasTeamPermission(permissions.BasePermission):
    """
    检查用户在团队中是否拥有特定权限
    
    使用方式：
    在 ViewSet 中设置 permission_classes 时，需要同时指定 required_permission
    
    class MyViewSet(viewsets.ModelViewSet):
        permission_classes = [HasTeamPermission]
        required_permission = 'view_instance'  # 需要的权限标识
    """
    message = '您没有执行此操作的权限'
    
    def has_permission(self, request, view):
        """基本权限检查"""
        if not (request.user and request.user.is_authenticated):
            return False
        
        # 超级管理员拥有所有权限
        if request.user.is_superuser:
            return True
        
        # 如果 view 没有指定 required_permission，则通过基本检查
        if not hasattr(view, 'required_permission'):
            return True
        
        return True
    
    def has_object_permission(self, request, view, obj):
        """对象级别权限检查"""
        # 超级管理员拥有所有权限
        if request.user.is_superuser:
            return True
        
        # 获取需要的权限
        required_permission = getattr(view, 'required_permission', None)
        if not required_permission:
            return True
        
        # 获取团队对象
        team = None
        if hasattr(obj, 'team'):
            team = obj.team
        elif hasattr(obj, 'members'):
            team = obj
        
        if not team:
            return False
        
        # 检查用户在该团队是否拥有所需权限
        return request.user.has_team_permission(team, required_permission)


class ReadOnly(permissions.BasePermission):
    """
    只读权限
    
    只允许安全方法（GET, HEAD, OPTIONS）
    """
    message = '此资源为只读'
    
    def has_permission(self, request, view):
        """只允许安全方法"""
        return request.method in permissions.SAFE_METHODS
    
    def has_object_permission(self, request, view, obj):
        """只允许安全方法"""
        return request.method in permissions.SAFE_METHODS


class IsOwnerOrReadOnly(permissions.BasePermission):
    """
    所有者可以修改，其他人只读
    
    对象必须有 owner 或 created_by 字段
    """
    message = '您只能修改自己创建的资源'
    
    def has_object_permission(self, request, view, obj):
        """检查是否为所有者"""
        # 安全方法允许所有人访问
        if request.method in permissions.SAFE_METHODS:
            return True
        
        # 超级管理员拥有所有权限
        if request.user.is_superuser:
            return True
        
        # 检查是否为所有者
        owner_field = getattr(obj, 'owner', None) or getattr(obj, 'created_by', None)
        if owner_field:
            return owner_field.id == request.user.id
        
        return False