"""
SQL客户端应用路由配置
定义SQL执行、查询历史相关的API端点
"""
from django.urls import path, include
from rest_framework.routers import DefaultRouter

from .views import (
    QueryExecutionView,
    SchemaView,
    QueryHistoryViewSet,
    ResultExportView
)

app_name = 'sqlclient'

# 创建路由器
router = DefaultRouter()
router.register('history', QueryHistoryViewSet, basename='history')

urlpatterns = [
    # 路由器注册的URL
    path('', include(router.urls)),
    
    # 结果导出端点
    path('results/<int:history_id>/export/', 
         ResultExportView.as_view(), 
         name='export-result'),
]