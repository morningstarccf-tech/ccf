"""
认证应用路由配置
"""
from django.urls import path, include
from rest_framework.routers import DefaultRouter
from rest_framework_simplejwt.views import TokenRefreshView

from .views import (
    CustomTokenObtainPairView,
    UserViewSet,
    RoleViewSet,
    PermissionViewSet,
    TeamViewSet,
)

# 创建路由器
router = DefaultRouter()

# 注册 ViewSet
router.register('users', UserViewSet, basename='user')
router.register('roles', RoleViewSet, basename='role')
router.register('permissions', PermissionViewSet, basename='permission')
router.register('teams', TeamViewSet, basename='team')

# URL 配置
urlpatterns = [
    # JWT 认证端点
    path('token/', CustomTokenObtainPairView.as_view(), name='token_obtain_pair'),
    path('token/refresh/', TokenRefreshView.as_view(), name='token_refresh'),
    
    # ViewSet 路由
    path('', include(router.urls)),
]