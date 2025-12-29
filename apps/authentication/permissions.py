"""
认证应用权限类
定义自定义权限规则,用于API访问控制
"""
from rest_framework import permissions


class IsTeamMember(permissions.BasePermission):
    """
    验证用户是否为指定团队成员
    """
    message = '您不是该团队的成员'

    def has_object_permission(self, request, view, obj):
        # 具体权限逻辑将在后续实现
        return True


class IsTeamAdmin(permissions.BasePermission):
    """
    验证用户是否为团队管理员
    """
    message = '您不是该团队的管理员'

    def has_object_permission(self, request, view, obj):
        # 具体权限逻辑将在后续实现
        return True