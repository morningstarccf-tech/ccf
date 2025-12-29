"""
备份管理的 URL 路由配置

定义备份策略和备份记录的 API 端点。
"""
from django.urls import path, include
from rest_framework.routers import DefaultRouter
from apps.backups.views import BackupStrategyViewSet, BackupRecordViewSet

app_name = 'backups'

# 创建路由器
router = DefaultRouter()

# 注册视图集
router.register('strategies', BackupStrategyViewSet, basename='strategy')
router.register('records', BackupRecordViewSet, basename='record')

# URL 配置
urlpatterns = [
    # API 路由
    path('', include(router.urls)),
]