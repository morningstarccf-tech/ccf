"""
实例应用路由配置

定义 MySQL 实例管理相关的 API 端点，使用嵌套路由支持实例下的数据库管理。
"""
from django.urls import path, include
from rest_framework.routers import DefaultRouter
from rest_framework_nested import routers

from apps.instances.views import MySQLInstanceViewSet, DatabaseViewSet
from apps.sqlclient.views import QueryExecutionView, SchemaView

app_name = 'instances'

# 主路由：实例管理
router = DefaultRouter()
router.register(r'', MySQLInstanceViewSet, basename='instance')

# 嵌套路由：实例下的数据库管理
# URL 格式：/instances/{instance_pk}/databases/
instances_router = routers.NestedDefaultRouter(
    router, 
    r'', 
    lookup='instance'
)
instances_router.register(
    r'databases', 
    DatabaseViewSet, 
    basename='instance-database'
)

urlpatterns = [
    path('', include(router.urls)),
    path('', include(instances_router.urls)),
    
    # SQL 执行和模式浏览端点
    path('<int:instance_id>/query/', QueryExecutionView.as_view(), name='instance-query'),
    path('<int:instance_id>/schema/', SchemaView.as_view(), name='instance-schema'),
]

# API 端点说明：
# 
# 实例管理：
# GET    /api/instances/                      - 获取实例列表
# POST   /api/instances/                      - 创建新实例
# GET    /api/instances/{id}/                 - 获取实例详情
# PUT    /api/instances/{id}/                 - 更新实例
# PATCH  /api/instances/{id}/                 - 部分更新实例
# DELETE /api/instances/{id}/                 - 删除实例
# POST   /api/instances/test-connection/      - 测试连接
# GET    /api/instances/{id}/dashboard/       - 获取仪表盘数据
# GET    /api/instances/{id}/databases/       - 获取数据库列表
# GET    /api/instances/{id}/metrics/         - 获取监控指标
# POST   /api/instances/{id}/refresh-status/  - 刷新实例状态
# POST   /api/instances/{id}/collect-metrics/ - 采集监控指标
# POST   /api/instances/{id}/query/           - 执行SQL查询
# GET    /api/instances/{id}/schema/          - 获取数据库结构
#
# 数据库管理（嵌套路由）：
# GET    /api/instances/{instance_id}/databases/                        - 获取数据库列表
# POST   /api/instances/{instance_id}/databases/                        - 创建数据库
# GET    /api/instances/{instance_id}/databases/{id}/                   - 获取数据库详情
# PUT    /api/instances/{instance_id}/databases/{id}/                   - 更新数据库
# PATCH  /api/instances/{instance_id}/databases/{id}/                   - 部分更新数据库
# DELETE /api/instances/{instance_id}/databases/{id}/                   - 删除数据库
# POST   /api/instances/{instance_id}/databases/{id}/update-statistics/ - 更新统计信息