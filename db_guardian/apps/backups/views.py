"""
备份管理的 API 视图

提供备份策略、备份记录的 CRUD、手动备份、恢复等功能。
"""
from django.utils import timezone
from django.http import FileResponse
from rest_framework import viewsets, status
from rest_framework.decorators import action
from rest_framework.response import Response
from rest_framework.permissions import IsAuthenticated
from django_filters.rest_framework import DjangoFilterBackend
from rest_framework.filters import SearchFilter, OrderingFilter
from pathlib import Path
import logging

from apps.backups.models import BackupStrategy, BackupRecord
from apps.backups.serializers import (
    BackupStrategySerializer,
    BackupStrategyCreateSerializer,
    BackupRecordSerializer,
    BackupRecordListSerializer,
    ManualBackupSerializer,
    RestoreSerializer,
)
from apps.backups.services import StrategyManager, RestoreExecutor
from apps.backups.tasks import execute_backup_task, verify_backup_integrity
from apps.authentication.permissions import IsTeamMember, IsTeamAdmin

logger = logging.getLogger(__name__)


class BackupStrategyViewSet(viewsets.ModelViewSet):
    """
    备份策略管理 ViewSet
    
    提供备份策略的完整 CRUD 功能，以及启用/禁用、同步等操作。
    
    权限说明：
    - list/retrieve: 团队成员可访问
    - create/update/delete: 团队管理员可操作
    """
    
    permission_classes = [IsAuthenticated, IsTeamMember]
    filter_backends = [DjangoFilterBackend, SearchFilter, OrderingFilter]
    filterset_fields = ['instance', 'is_enabled', 'backup_type']
    search_fields = ['name']
    ordering_fields = ['created_at', 'name']
    ordering = ['-created_at']
    
    def get_queryset(self):
        """
        根据用户权限过滤策略列表
        
        - 超级管理员：查看所有策略
        - 普通用户：仅查看所属团队的策略
        """
        user = self.request.user
        
        if user.is_superuser:
            return BackupStrategy.objects.all().select_related(
                'instance', 'instance__team', 'created_by'
            )
        
        # 获取用户所属的所有团队
        user_teams = user.teams.all()
        return BackupStrategy.objects.filter(
            instance__team__in=user_teams
        ).select_related(
            'instance', 'instance__team', 'created_by'
        )
    
    def get_serializer_class(self):
        """根据动作返回不同的序列化器"""
        if self.action in ['create', 'update', 'partial_update']:
            return BackupStrategyCreateSerializer
        return BackupStrategySerializer
    
    def get_permissions(self):
        """根据动作设置不同的权限"""
        if self.action in ['create', 'update', 'partial_update', 'destroy']:
            # 修改和删除需要管理员权限
            return [IsAuthenticated(), IsTeamAdmin()]
        return super().get_permissions()
    
    @action(detail=True, methods=['post'], url_path='enable')
    def enable(self, request, pk=None):
        """
        启用备份策略
        
        POST /strategies/{id}/enable/
        """
        strategy = self.get_object()
        
        if strategy.is_enabled:
            return Response({
                'success': False,
                'message': '策略已经是启用状态'
            }, status=status.HTTP_400_BAD_REQUEST)
        
        strategy.is_enabled = True
        strategy.save()
        
        # 同步到 Celery Beat
        try:
            StrategyManager.sync_to_celery_beat()
            return Response({
                'success': True,
                'message': '策略已启用并同步到调度器'
            })
        except Exception as e:
            logger.exception(f"Failed to sync strategy: {str(e)}")
            return Response({
                'success': False,
                'message': f'策略已启用，但同步失败: {str(e)}'
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    
    @action(detail=True, methods=['post'], url_path='disable')
    def disable(self, request, pk=None):
        """
        禁用备份策略
        
        POST /strategies/{id}/disable/
        """
        strategy = self.get_object()
        
        if not strategy.is_enabled:
            return Response({
                'success': False,
                'message': '策略已经是禁用状态'
            }, status=status.HTTP_400_BAD_REQUEST)
        
        strategy.is_enabled = False
        strategy.save()
        
        # 同步到 Celery Beat（删除定时任务）
        try:
            StrategyManager.sync_to_celery_beat()
            return Response({
                'success': True,
                'message': '策略已禁用并从调度器中移除'
            })
        except Exception as e:
            logger.exception(f"Failed to sync strategy: {str(e)}")
            return Response({
                'success': False,
                'message': f'策略已禁用，但同步失败: {str(e)}'
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    
    @action(detail=False, methods=['post'], url_path='sync')
    def sync(self, request):
        """
        手动同步所有策略到 Celery Beat
        
        POST /strategies/sync/
        """
        try:
            result = StrategyManager.sync_to_celery_beat()
            return Response({
                'success': True,
                'message': '策略同步成功',
                'result': result
            })
        except Exception as e:
            logger.exception(f"Failed to sync strategies: {str(e)}")
            return Response({
                'success': False,
                'message': f'策略同步失败: {str(e)}'
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


class BackupRecordViewSet(viewsets.ModelViewSet):
    """
    备份记录管理 ViewSet
    
    提供备份记录的查询、下载、删除、恢复、验证等功能。
    
    权限说明：
    - list/retrieve/download: 团队成员可访问
    - destroy: 团队管理员可操作
    - restore: 需要特殊权限验证
    """
    
    permission_classes = [IsAuthenticated, IsTeamMember]
    filter_backends = [DjangoFilterBackend, SearchFilter, OrderingFilter]
    filterset_fields = ['instance', 'status', 'backup_type']
    search_fields = ['database_name']
    ordering_fields = ['created_at', 'start_time', 'file_size_mb']
    ordering = ['-created_at']
    
    def get_queryset(self):
        """
        根据用户权限过滤备份记录
        
        - 超级管理员：查看所有记录
        - 普通用户：仅查看所属团队的记录
        """
        user = self.request.user
        
        if user.is_superuser:
            return BackupRecord.objects.all().select_related(
                'instance', 'instance__team', 'strategy', 'created_by'
            )
        
        # 获取用户所属的所有团队
        user_teams = user.teams.all()
        return BackupRecord.objects.filter(
            instance__team__in=user_teams
        ).select_related(
            'instance', 'instance__team', 'strategy', 'created_by'
        )
    
    def get_serializer_class(self):
        """根据动作返回不同的序列化器"""
        if self.action == 'list':
            return BackupRecordListSerializer
        return BackupRecordSerializer
    
    def get_permissions(self):
        """根据动作设置不同的权限"""
        if self.action in ['destroy', 'restore']:
            # 删除和恢复需要管理员权限
            return [IsAuthenticated(), IsTeamAdmin()]
        return super().get_permissions()
    
    def destroy(self, request, *args, **kwargs):
        """
        删除备份记录及其文件
        
        DELETE /records/{id}/
        """
        record = self.get_object()
        
        # 删除文件
        if record.file_path and Path(record.file_path).exists():
            try:
                Path(record.file_path).unlink()
                logger.info(f"已删除备份文件: {record.file_path}")
            except Exception as e:
                logger.error(f"删除备份文件失败: {str(e)}")
        
        # 删除记录
        record.delete()
        
        return Response({
            'success': True,
            'message': '备份记录已删除'
        }, status=status.HTTP_204_NO_CONTENT)
    
    @action(detail=True, methods=['get'], url_path='download')
    def download(self, request, pk=None):
        """
        下载备份文件
        
        GET /records/{id}/download/
        """
        record = self.get_object()
        
        if record.status != 'success':
            return Response({
                'success': False,
                'message': '只能下载成功的备份文件'
            }, status=status.HTTP_400_BAD_REQUEST)
        
        if not record.file_path:
            return Response({
                'success': False,
                'message': '备份文件路径为空'
            }, status=status.HTTP_404_NOT_FOUND)
        
        file_path = Path(record.file_path)
        
        if not file_path.exists():
            return Response({
                'success': False,
                'message': '备份文件不存在'
            }, status=status.HTTP_404_NOT_FOUND)
        
        # 返回文件
        try:
            response = FileResponse(
                open(file_path, 'rb'),
                as_attachment=True,
                filename=file_path.name
            )
            return response
        except Exception as e:
            logger.exception(f"Failed to download backup: {str(e)}")
            return Response({
                'success': False,
                'message': f'文件下载失败: {str(e)}'
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    
    @action(detail=True, methods=['post'], url_path='restore')
    def restore(self, request, pk=None):
        """
        执行恢复操作
        
        POST /records/{id}/restore/
        Body: {
            "target_database": "db_name",  // 可选
            "confirm": true  // 必须
        }
        """
        record = self.get_object()
        
        # 验证请求数据
        serializer = RestoreSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        
        if record.status != 'success':
            return Response({
                'success': False,
                'message': '只能从成功的备份中恢复'
            }, status=status.HTTP_400_BAD_REQUEST)
        
        target_database = serializer.validated_data.get('target_database')
        
        # 执行恢复
        try:
            executor = RestoreExecutor(record.instance)
            result = executor.execute_restore(
                record.file_path,
                target_database
            )
            
            if result['success']:
                logger.info(f"恢复成功: 备份ID={record.id}, 用户={request.user.username}")
                return Response({
                    'success': True,
                    'message': '数据恢复成功'
                })
            else:
                return Response({
                    'success': False,
                    'message': result.get('error_message', '恢复失败')
                }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        
        except Exception as e:
            logger.exception(f"恢复失败: {str(e)}")
            return Response({
                'success': False,
                'message': f'恢复失败: {str(e)}'
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    
    @action(detail=True, methods=['post'], url_path='verify')
    def verify(self, request, pk=None):
        """
        验证备份文件完整性
        
        POST /records/{id}/verify/
        """
        record = self.get_object()
        
        if record.status != 'success':
            return Response({
                'success': False,
                'message': '只能验证成功的备份文件'
            }, status=status.HTTP_400_BAD_REQUEST)
        
        # 异步执行验证任务
        task = verify_backup_integrity.delay(record.id)
        
        return Response({
            'success': True,
            'message': '验证任务已创建',
            'task_id': task.id
        })


# 在 MySQLInstanceViewSet 中添加手动备份动作
# 这部分代码需要添加到 apps/instances/views.py 的 MySQLInstanceViewSet 类中
"""
@action(detail=True, methods=['post'], url_path='backup')
def backup(self, request, pk=None):
    '''
    手动触发备份
    
    POST /instances/{id}/backup/
    Body: {
        "database_name": "db_name",  // 可选
        "compress": true  // 可选，默认true
    }
    '''
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
"""
