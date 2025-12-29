"""
MySQL 实例管理的 API 视图

提供实例的 CRUD、连接测试、监控数据等功能。
"""
from django.utils import timezone
from django.db.models import Count, Sum
from rest_framework import viewsets, status
from rest_framework.decorators import action
from rest_framework.response import Response
from rest_framework.permissions import IsAuthenticated
from django_filters.rest_framework import DjangoFilterBackend
from rest_framework.filters import SearchFilter, OrderingFilter

from apps.instances.models import MySQLInstance, Database, MonitoringMetrics
from apps.instances.serializers import (
    MySQLInstanceSerializer,
    MySQLInstanceCreateSerializer,
    MySQLInstanceUpdateSerializer,
    ConnectionTestSerializer,
    DatabaseSerializer,
    MonitoringMetricsSerializer,
    DashboardSerializer,
)
from apps.instances.services import HealthChecker, MetricsCollector
from apps.authentication.permissions import IsTeamMember, IsTeamAdmin
import pymysql
import logging

logger = logging.getLogger(__name__)


class MySQLInstanceViewSet(viewsets.ModelViewSet):
    """
    MySQL 实例管理 ViewSet
    
    提供实例的完整 CRUD 功能，以及连接测试、仪表盘等自定义动作。
    
    权限说明：
    - list/retrieve: 团队成员可访问
    - create: 团队管理员和开发人员可创建
    - update/delete: 团队管理员可操作
    """
    
    permission_classes = [IsAuthenticated, IsTeamMember]
    filter_backends = [DjangoFilterBackend, SearchFilter, OrderingFilter]
    filterset_fields = ['team', 'status']
    search_fields = ['alias', 'host', 'description']
    ordering_fields = ['created_at', 'alias', 'status']
    ordering = ['-created_at']
    
    def get_queryset(self):
        """
        根据用户权限过滤实例列表
        
        - 超级管理员：查看所有实例
        - 普通用户：仅查看所属团队的实例
        """
        user = self.request.user
        
        if user.is_superuser:
            return MySQLInstance.objects.all().select_related(
                'team', 'created_by'
            ).prefetch_related('databases')
        
        # 获取用户所属的所有团队
        user_teams = user.teams.all()
        return MySQLInstance.objects.filter(
            team__in=user_teams
        ).select_related(
            'team', 'created_by'
        ).prefetch_related('databases')
    
    def get_serializer_class(self):
        """根据动作返回不同的序列化器"""
        if self.action == 'create':
            return MySQLInstanceCreateSerializer
        elif self.action in ['update', 'partial_update']:
            return MySQLInstanceUpdateSerializer
        return MySQLInstanceSerializer
    
    def get_permissions(self):
        """根据动作设置不同的权限"""
        if self.action in ['update', 'partial_update', 'destroy']:
            # 修改和删除需要管理员权限
            return [IsAuthenticated(), IsTeamAdmin()]
        return super().get_permissions()
    
    def perform_create(self, serializer):
        """创建实例时设置创建者"""
        serializer.save(created_by=self.request.user)
    
    @action(detail=False, methods=['post'], url_path='test-connection')
    def test_connection(self, request):
        """
        测试 MySQL 连接
        
        POST /instances/test-connection/
        Body: {
            "host": "localhost",
            "port": 3306,
            "username": "root",
            "password": "password"
        }
        """
        serializer = ConnectionTestSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        
        data = serializer.validated_data
        
        try:
            connection = pymysql.connect(
                host=data['host'],
                port=data['port'],
                user=data['username'],
                password=data['password'],
                connect_timeout=5
            )
            
            with connection.cursor() as cursor:
                # 获取版本
                cursor.execute("SELECT VERSION()")
                version = cursor.fetchone()[0]
                
                # 获取字符集
                cursor.execute("SHOW VARIABLES LIKE 'character_set_server'")
                charset_result = cursor.fetchone()
                charset = charset_result[1] if charset_result else 'unknown'
            
            connection.close()
            
            return Response({
                'success': True,
                'message': '连接成功',
                'version': version,
                'charset': charset
            })
        
        except pymysql.Error as e:
            return Response({
                'success': False,
                'message': f'连接失败: {str(e)}'
            }, status=status.HTTP_400_BAD_REQUEST)
        
        except Exception as e:
            logger.exception(f"Connection test error: {str(e)}")
            return Response({
                'success': False,
                'message': f'测试异常: {str(e)}'
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    
    @action(detail=True, methods=['get'], url_path='dashboard')
    def dashboard(self, request, pk=None):
        """
        获取实例仪表盘数据
        
        GET /instances/{id}/dashboard/
        
        返回：
        - 实例基本信息
        - 数据库统计
        - 最新监控指标
        - 24小时监控历史
        """
        instance = self.get_object()
        
        # 数据库统计
        db_stats = instance.databases.aggregate(
            count=Count('id'),
            total_size=Sum('size_mb')
        )
        
        # 最新监控指标
        latest_metrics = instance.metrics.first()
        
        # 24小时监控历史
        twenty_four_hours_ago = timezone.now() - timezone.timedelta(hours=24)
        metrics_history = instance.metrics.filter(
            timestamp__gte=twenty_four_hours_ago
        ).order_by('-timestamp')[:100]
        
        # 连接信息
        connection_info = {
            'host': instance.host,
            'port': instance.port,
            'username': instance.username,
            'charset': instance.charset,
        }
        
        dashboard_data = {
            'instance_id': instance.id,
            'alias': instance.alias,
            'status': instance.status,
            'version': instance.version,
            'database_count': db_stats['count'] or 0,
            'total_size_mb': db_stats['total_size'] or 0,
            'current_metrics': latest_metrics,
            'metrics_history': metrics_history,
            'connection_info': connection_info,
        }
        
        serializer = DashboardSerializer(dashboard_data)
        return Response(serializer.data)
    
    @action(detail=True, methods=['get'], url_path='databases')
    def databases(self, request, pk=None):
        """
        获取实例的数据库列表
        
        GET /instances/{id}/databases/
        """
        instance = self.get_object()
        databases = instance.databases.all().order_by('name')
        serializer = DatabaseSerializer(databases, many=True)
        return Response(serializer.data)
    
    @action(detail=True, methods=['get'], url_path='metrics')
    def metrics(self, request, pk=None):
        """
        获取实例监控指标
        
        GET /instances/{id}/metrics/?hours=24
        
        参数：
        - hours: 查询最近多少小时的数据（默认24小时）
        """
        instance = self.get_object()
        hours = int(request.query_params.get('hours', 24))
        
        start_time = timezone.now() - timezone.timedelta(hours=hours)
        metrics = instance.metrics.filter(
            timestamp__gte=start_time
        ).order_by('-timestamp')
        
        serializer = MonitoringMetricsSerializer(metrics, many=True)
        return Response(serializer.data)
    
    @action(detail=True, methods=['post'], url_path='refresh-status')
    def refresh_status(self, request, pk=None):
        """
        刷新实例状态
        
        POST /instances/{id}/refresh-status/
        
        立即执行健康检查并更新实例状态
        """
        instance = self.get_object()
        
        # 执行健康检查
        is_healthy, message, info = HealthChecker.check_instance(instance)
        
        # 更新实例状态和版本信息
        if is_healthy:
            instance.status = 'online'
            if 'version' in info and not instance.version:
                instance.version = info['version']
        else:
            instance.status = 'error' if 'timeout' not in message.lower() else 'offline'
        
        instance.last_check_time = timezone.now()
        instance.save(update_fields=['status', 'last_check_time', 'version'])
        
        return Response({
            'status': instance.status,
            'message': message,
            'info': info,
            'last_check_time': instance.last_check_time
        })
    
    @action(detail=True, methods=['post'], url_path='collect-metrics')
    def collect_metrics(self, request, pk=None):
        """
        采集实例监控指标
        
        POST /instances/{id}/collect-metrics/
        
        立即采集并保存监控指标
        """
        instance = self.get_object()
        
        # 采集指标
        metrics = MetricsCollector.collect_metrics(instance)
        
        if not metrics:
            return Response({
                'success': False,
                'message': '指标采集失败'
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        
        # 保存指标
        success = MetricsCollector.save_metrics(instance, metrics)
        
        if success:
            return Response({
                'success': True,
                'message': '指标采集成功',
                'metrics': metrics
            })
        else:
            return Response({
                'success': False,
                'message': '指标保存失败'
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    
    @action(detail=True, methods=['post'], url_path='backup')
    def backup(self, request, pk=None):
        """
        手动触发备份
        
        POST /instances/{id}/backup/
        Body: {
            "database_name": "db_name",  // 可选
            "compress": true  // 可选，默认true
        }
        """
        from apps.backups.serializers import ManualBackupSerializer
        from apps.backups.tasks import execute_backup_task
        
        instance = self.get_object()
        
        # 验证请求数据
        serializer = ManualBackupSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        
        database_name = serializer.validated_data.get('database_name')
        compress = serializer.validated_data.get('compress', True)
        
        # 创建异步备份任务
        task = execute_backup_task.delay(
            instance_id=instance.id,
            database_name=database_name,
            user_id=request.user.id
        )
        
        logger.info(f"手动备份任务已创建: 实例={instance.alias}, 任务ID={task.id}")
        
        return Response({
            'success': True,
            'message': '备份任务已创建',
            'task_id': task.id
        })


class DatabaseViewSet(viewsets.ModelViewSet):
    """
    数据库管理 ViewSet
    
    嵌套在实例下：/instances/{instance_id}/databases/
    
    提供数据库的 CRUD 功能。
    """
    
    serializer_class = DatabaseSerializer
    permission_classes = [IsAuthenticated, IsTeamMember]
    filter_backends = [SearchFilter, OrderingFilter]
    search_fields = ['name']
    ordering_fields = ['name', 'size_mb', 'created_at']
    ordering = ['name']
    
    def get_queryset(self):
        """根据实例ID过滤数据库"""
        instance_id = self.kwargs.get('instance_pk')
        if instance_id:
            return Database.objects.filter(
                instance_id=instance_id
            ).select_related('instance')
        return Database.objects.none()
    
    def get_permissions(self):
        """根据动作设置不同的权限"""
        if self.action in ['create', 'update', 'partial_update', 'destroy']:
            # 修改需要管理员权限
            return [IsAuthenticated(), IsTeamAdmin()]
        return super().get_permissions()
    
    @action(detail=True, methods=['post'], url_path='update-statistics')
    def update_statistics(self, request, instance_pk=None, pk=None):
        """
        更新数据库统计信息
        
        POST /instances/{instance_id}/databases/{id}/update-statistics/
        
        更新数据库大小和表数量
        """
        database = self.get_object()
        
        try:
            database.update_statistics()
            serializer = self.get_serializer(database)
            return Response({
                'success': True,
                'message': '统计信息更新成功',
                'database': serializer.data
            })
        except Exception as e:
            logger.exception(f"Failed to update statistics for {database}: {str(e)}")
            return Response({
                'success': False,
                'message': f'统计信息更新失败: {str(e)}'
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
