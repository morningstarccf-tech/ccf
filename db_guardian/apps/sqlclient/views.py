"""
SQL客户端应用视图
提供SQL执行、模式浏览、历史记录和结果导出的API端点
"""
from rest_framework import viewsets, status
from rest_framework.decorators import action
from rest_framework.response import Response
from rest_framework.views import APIView
from rest_framework.permissions import IsAuthenticated
from django.http import HttpResponse
from django.shortcuts import get_object_or_404
import logging

from apps.instances.models import MySQLInstance
from apps.authentication.permissions import HasTeamPermission, IsTeamMember
from .models import QueryHistory
from .serializers import (
    QueryExecutionSerializer,
    QueryResultSerializer,
    QueryHistorySerializer,
    QueryHistoryDetailSerializer,
    SchemaSerializer,
    ExportRequestSerializer
)
from .services import QueryExecutor, SchemaExplorer, ResultExporter

logger = logging.getLogger(__name__)


class QueryExecutionView(APIView):
    """
    SQL执行视图
    
    提供SQL查询执行功能
    """
    permission_classes = [IsAuthenticated, IsTeamMember]
    
    def post(self, request, instance_id):
        """
        执行SQL查询
        
        POST /api/instances/{instance_id}/query/
        
        Request Body:
        {
            "sql": "SELECT * FROM users",
            "database": "mydb",
            "timeout": 30,
            "apply_limit": true,
            "max_rows": 1000
        }
        
        Response:
        {
            "success": true,
            "sql_type": "SELECT",
            "rows_affected": 10,
            "execution_time_ms": 123,
            "columns": ["id", "name", "email"],
            "data": [{...}],
            "message": "执行成功",
            "history_id": 123,
            "warnings": []
        }
        """
        # 获取实例
        instance = get_object_or_404(MySQLInstance, id=instance_id)
        
        # 检查团队权限
        self.check_object_permissions(request, instance)
        
        # 验证请求数据
        serializer = QueryExecutionSerializer(data=request.data)
        if not serializer.is_valid():
            return Response(
                {'success': False, 'message': '请求参数错误', 'errors': serializer.errors},
                status=status.HTTP_400_BAD_REQUEST
            )
        
        # 执行查询
        executor = QueryExecutor(instance, request.user)
        # 执行器负责校验、权限与历史记录写入。
        result = executor.execute_query(
            sql=serializer.validated_data['sql'],
            database=serializer.validated_data.get('database'),
            timeout=serializer.validated_data.get('timeout', 30),
            apply_limit=serializer.validated_data.get('apply_limit', True),
            max_rows=serializer.validated_data.get('max_rows', 1000)
        )
        
        # 返回结果
        if result['success']:
            result_serializer = QueryResultSerializer(result)
            return Response(result_serializer.data, status=status.HTTP_200_OK)
        else:
            return Response(result, status=status.HTTP_400_BAD_REQUEST)


class SchemaView(APIView):
    """
    数据库模式浏览视图
    
    提供数据库结构查询功能
    """
    permission_classes = [IsAuthenticated, IsTeamMember]
    
    def get(self, request, instance_id):
        """
        获取数据库结构
        
        GET /api/instances/{instance_id}/schema/
        GET /api/instances/{instance_id}/schema/?database=mydb
        
        Response:
        {
            "databases": [
                {
                    "name": "mydb",
                    "tables": [
                        {
                            "name": "users",
                            "type": "BASE TABLE",
                            "engine": "InnoDB",
                            "rows": 1000,
                            "columns": [...],
                            "indexes": [...]
                        }
                    ]
                }
            ]
        }
        """
        # 获取实例
        instance = get_object_or_404(MySQLInstance, id=instance_id)
        
        # 检查团队权限
        self.check_object_permissions(request, instance)
        
        # 获取数据库参数
        database = request.query_params.get('database')
        
        # 获取模式信息
        explorer = SchemaExplorer(instance)
        # 结构浏览器返回数据库/表/列的层级结构。
        schema_data = explorer.get_database_schema(database)
        
        # 检查是否有错误
        if 'error' in schema_data:
            return Response(
                {'success': False, 'message': f'获取数据库结构失败: {schema_data["error"]}'},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )
        
        # 序列化并返回
        serializer = SchemaSerializer(schema_data)
        return Response(serializer.data, status=status.HTTP_200_OK)


class QueryHistoryViewSet(viewsets.ReadOnlyModelViewSet):
    """
    SQL执行历史视图集
    
    提供查询历史的列表和详情查看功能（只读）
    """
    permission_classes = [IsAuthenticated]
    serializer_class = QueryHistorySerializer
    
    def get_queryset(self):
        """
        获取查询集
        
        过滤条件：
        - instance: 实例ID
        - database: 数据库名称
        - sql_type: SQL类型
        - status: 执行状态
        - start_date: 开始日期
        - end_date: 结束日期
        """
        queryset = QueryHistory.objects.select_related(
            'instance', 'executed_by'
        )
        
        # 非超级管理员只能查看自己的历史
        if not self.request.user.is_superuser:
            queryset = queryset.filter(executed_by=self.request.user)
        
        # 过滤条件
        instance_id = self.request.query_params.get('instance')
        if instance_id:
            queryset = queryset.filter(instance_id=instance_id)
        
        database = self.request.query_params.get('database')
        if database:
            queryset = queryset.filter(database_name=database)
        
        sql_type = self.request.query_params.get('sql_type')
        if sql_type:
            queryset = queryset.filter(sql_type=sql_type)
        
        status_filter = self.request.query_params.get('status')
        if status_filter:
            queryset = queryset.filter(status=status_filter)
        
        start_date = self.request.query_params.get('start_date')
        if start_date:
            queryset = queryset.filter(executed_at__gte=start_date)
        
        end_date = self.request.query_params.get('end_date')
        if end_date:
            queryset = queryset.filter(executed_at__lte=end_date)
        
        return queryset
    
    def get_serializer_class(self):
        """根据动作选择序列化器"""
        if self.action == 'retrieve':
            return QueryHistoryDetailSerializer
        return QueryHistorySerializer


class ResultExportView(APIView):
    """
    查询结果导出视图
    
    提供查询结果的导出功能
    """
    permission_classes = [IsAuthenticated]
    
    def get(self, request, history_id):
        """
        导出查询结果
        
        GET /api/sqlclient/results/{history_id}/export/?format=csv
        
        Response: 文件下载
        """
        # 获取历史记录
        history = get_object_or_404(QueryHistory, id=history_id)
        
        # 权限检查：只能导出自己的查询结果
        if not request.user.is_superuser and history.executed_by != request.user:
            return Response(
                {'success': False, 'message': '您只能导出自己的查询结果'},
                status=status.HTTP_403_FORBIDDEN
            )
        
        # 检查是否有缓存的结果
        if not history.result_cached:
            return Response(
                {'success': False, 'message': '该查询结果未缓存，无法导出'},
                status=status.HTTP_404_NOT_FOUND
            )
        
        # 从缓存获取结果
        result_data = ResultExporter.get_cached_result(history_id)
        if not result_data:
            return Response(
                {'success': False, 'message': '查询结果已过期，请重新执行查询'},
                status=status.HTTP_410_GONE
            )
        
        # 获取导出格式
        export_format = request.query_params.get('format', 'csv')
        
        if export_format == 'csv':
            # 导出为CSV
            csv_content = ResultExporter.export_to_csv(result_data)
            
            # 生成文件名
            filename = f'query_result_{history_id}.csv'
            
            # 返回文件响应
            response = HttpResponse(csv_content, content_type='text/csv')
            response['Content-Disposition'] = f'attachment; filename="{filename}"'
            return response
        else:
            return Response(
                {'success': False, 'message': f'不支持的导出格式: {export_format}'},
                status=status.HTTP_400_BAD_REQUEST
            )
