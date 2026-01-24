"""
备份管理的 API 视图

提供备份策略、备份记录的 CRUD、手动备份、恢复等功能。
"""
from django.utils import timezone
from django.http import FileResponse
from django.conf import settings
from rest_framework import viewsets, status
from rest_framework.decorators import action
from rest_framework.authentication import SessionAuthentication
from rest_framework_simplejwt.authentication import JWTAuthentication
from rest_framework.response import Response
from rest_framework.permissions import IsAuthenticated
from rest_framework.parsers import MultiPartParser, FormParser
from django_filters.rest_framework import DjangoFilterBackend
from rest_framework.filters import SearchFilter, OrderingFilter
from pathlib import Path
import logging
from uuid import uuid4

from apps.backups.models import BackupStrategy, BackupRecord, BackupOneOffTask
from apps.backups.serializers import (
    BackupStrategySerializer,
    BackupStrategyCreateSerializer,
    BackupRecordSerializer,
    BackupRecordListSerializer,
    ManualBackupSerializer,
    RestoreSerializer,
    RestoreUploadSerializer,
    BackupOneOffTaskSerializer,
    BackupOneOffTaskCreateSerializer,
)
from apps.backups.services import (
    StrategyManager,
    RestoreExecutor,
    RemoteExecutor,
    RemoteStorageClient,
    ObjectStorageUploader
)
from apps.backups.tasks import execute_backup_task, verify_backup_integrity
from apps.authentication.permissions import IsTeamMember, IsTeamAdmin
from apps.instances.models import MySQLInstance

logger = logging.getLogger(__name__)


def _infer_backup_filenames(record):
    # 汇总所有存储位置的可能文件名，提高命中率。
    names = []
    for path_value in [record.file_path, record.remote_path, record.object_storage_path]:
        if not path_value:
            continue
        if path_value.startswith('oss://'):
            stripped = path_value[len('oss://'):]
            _, _, key = stripped.partition('/')
            if key:
                name = Path(key).name
                if name not in ('', '.', '..'):
                    names.append(name)
                    continue
        name = Path(path_value).name
        if name not in ('', '.', '..'):
            names.append(name)

    unique = []
    for name in names:
        if name not in unique:
            unique.append(name)
    if unique:
        return unique

    # 回退为根据记录元数据生成文件名。
    timestamp = None
    if record.start_time:
        timestamp = record.start_time.strftime('%Y%m%d_%H%M%S')
    elif record.created_at:
        timestamp = record.created_at.strftime('%Y%m%d_%H%M%S')

    db_suffix = record.database_name or 'all'
    alias = record.instance.alias if record.instance else 'backup'
    candidates = []
    if timestamp:
        base = f"{alias}_{db_suffix}_{timestamp}.sql"
        if record.strategy and not record.strategy.compress:
            candidates.append(base)
            candidates.append(base + '.gz')
        else:
            candidates.append(base + '.gz')
            candidates.append(base)
    candidates.append(f"backup_{record.id}.sql.gz")
    candidates.append(f"backup_{record.id}.sql")

    unique = []
    for name in candidates:
        if name not in unique:
            unique.append(name)
    return unique


def _prepare_backup_download_path(record):
    # 先尝试本地文件，再尝试远程存储，最后尝试对象存储。
    filenames = _infer_backup_filenames(record)

    if record.file_path:
        file_path = Path(record.file_path)
        if file_path.exists() and file_path.is_file():
            return file_path
        if file_path.exists() and file_path.is_dir():
            for name in filenames:
                candidate = file_path / name
                if candidate.exists() and candidate.is_file():
                    return candidate

    backup_root = Path(getattr(settings, 'BACKUP_STORAGE_PATH', settings.BASE_DIR / 'backups'))
    temp_dir = backup_root / 'tmp'
    temp_dir.mkdir(parents=True, exist_ok=True)
    temp_path = None
    if filenames:
        temp_path = temp_dir / filenames[0]

    if record.remote_path:
        try:
            remote_candidates = []
            remote_path = record.remote_path
            if Path(remote_path).suffix:
                remote_candidates.append(remote_path)
            else:
                for name in filenames:
                    remote_candidates.append(str(Path(remote_path) / name))

            for remote_candidate in remote_candidates:
                # 将远程候选文件下载到临时目录。
                temp_path = temp_dir / Path(remote_candidate).name
                if record.remote_protocol:
                    client = RemoteStorageClient(
                        protocol=record.remote_protocol,
                        host=record.remote_host,
                        port=record.remote_port,
                        user=record.remote_user,
                        password=record.get_decrypted_remote_password(),
                        key_path=record.remote_key_path
                    )
                    client.download(remote_candidate, temp_path)
                else:
                    executor = RemoteExecutor(record.instance)
                    executor.download(remote_candidate, temp_path)
                if temp_path.exists() and temp_path.is_file():
                    return temp_path
        except Exception as exc:
            logger.warning(f"远程备份下载失败: {exc}")

    if record.object_storage_path:
        oss_config = None
        if record.strategy and (
            record.strategy.oss_endpoint
            or record.strategy.oss_access_key_id
            or record.strategy.oss_bucket
        ):
            # 如策略配置了 OSS 信息，优先使用策略级别配置。
            oss_config = {
                'endpoint': record.strategy.oss_endpoint,
                'access_key_id': record.strategy.oss_access_key_id,
                'access_key_secret': record.strategy.get_decrypted_oss_access_key_secret(),
                'bucket': record.strategy.oss_bucket,
                'prefix': record.strategy.oss_prefix
            }
        uploader = ObjectStorageUploader(config=oss_config)
        try:
            object_path = record.object_storage_path
            object_candidates = []
            if object_path.endswith('/'):
                for name in filenames:
                    object_candidates.append(object_path.rstrip('/') + '/' + name)
            else:
                object_candidates.append(object_path)

            for object_candidate in object_candidates:
                # 下载对象存储候选文件到临时目录以便响应/恢复。
                temp_path = temp_dir / Path(object_candidate).name
                uploader.download(object_candidate, temp_path)
                if temp_path.exists() and temp_path.is_file():
                    return temp_path
        except Exception as exc:
            logger.warning(f"OSS 备份下载失败: {exc}")

    return None


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
    authentication_classes = [JWTAuthentication, SessionAuthentication]
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
        if self.action in ['destroy', 'restore', 'restore_upload']:
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
                # 仅删除本地文件；远程/对象存储由其他流程处理。
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
        
        file_path = _prepare_backup_download_path(record)
        if not file_path:
            return Response({
                'success': False,
                'message': '备份文件不存在或无法下载'
            }, status=status.HTTP_404_NOT_FOUND)
        
        # 返回文件
        try:
            # 以流方式返回，避免一次性读入内存。
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

        if record.backup_type in ['hot', 'cold', 'incremental']:
            return Response({
                'success': False,
                'message': '物理备份恢复需离线操作，暂不支持在线恢复'
            }, status=status.HTTP_400_BAD_REQUEST)
        
        target_database = serializer.validated_data.get('target_database')
        
        restore_path = _prepare_backup_download_path(record)
        if not restore_path:
            return Response({
                'success': False,
                'message': '备份文件不存在或无法下载'
            }, status=status.HTTP_404_NOT_FOUND)

        # 执行恢复
        try:
            executor = RestoreExecutor(record.instance)
            result = executor.execute_restore(
                str(restore_path),
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
        finally:
            try:
                # 如果是临时下载文件则清理。
                if restore_path and (
                    not record.file_path
                    or Path(record.file_path).resolve() != restore_path.resolve()
                ):
                    if restore_path.exists():
                        restore_path.unlink()
            except Exception as exc:
                logger.warning(f"清理临时恢复文件失败: {exc}")

    @action(
        detail=False,
        methods=['post'],
        url_path='restore-upload',
        parser_classes=[MultiPartParser, FormParser]
    )
    def restore_upload(self, request):
        """
        上传备份文件并执行恢复

        POST /records/restore-upload/
        FormData: instance_id, backup_file, target_database(optional), confirm
        """
        serializer = RestoreUploadSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)

        instance_id = serializer.validated_data['instance_id']
        backup_file = serializer.validated_data['backup_file']
        target_database = serializer.validated_data.get('target_database')

        try:
            instance = MySQLInstance.objects.get(id=instance_id)
        except MySQLInstance.DoesNotExist:
            return Response({
                'success': False,
                'message': 'MySQL 实例不存在'
            }, status=status.HTTP_404_NOT_FOUND)

        if not request.user.is_superuser:
            admin_checker = IsTeamAdmin()
            # 恢复操作需要实例级管理员权限。
            if not admin_checker.has_object_permission(request, self, instance):
                return Response({
                    'success': False,
                    'message': '无权限恢复该实例'
                }, status=status.HTTP_403_FORBIDDEN)

        backup_root = Path(getattr(settings, 'BACKUP_STORAGE_PATH', settings.BASE_DIR / 'backups'))
        temp_dir = backup_root / 'uploads'
        temp_dir.mkdir(parents=True, exist_ok=True)
        safe_name = Path(backup_file.name).name
        temp_path = temp_dir / f"restore_{uuid4().hex}_{safe_name}"

        try:
            with open(temp_path, 'wb') as f_out:
                # 分块写入，避免占用过多内存。
                for chunk in backup_file.chunks():
                    f_out.write(chunk)

            executor = RestoreExecutor(instance)
            result = executor.execute_restore(str(temp_path), target_database)
            if result.get('success'):
                return Response({
                    'success': True,
                    'message': '数据恢复成功'
                })

            return Response({
                'success': False,
                'message': result.get('error_message', '恢复失败')
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        except Exception as exc:
            logger.exception(f"上传恢复失败: {exc}")
            return Response({
                'success': False,
                'message': f'恢复失败: {str(exc)}'
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        finally:
            try:
                # 始终清理上传的临时文件。
                if temp_path.exists():
                    temp_path.unlink()
            except Exception as exc:
                logger.warning(f"清理上传文件失败: {exc}")
    
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


class BackupOneOffTaskViewSet(viewsets.ModelViewSet):
    """
    一次性定时任务 ViewSet
    """

    permission_classes = [IsAuthenticated, IsTeamMember]
    filter_backends = [DjangoFilterBackend, SearchFilter, OrderingFilter]
    filterset_fields = ['instance', 'status', 'backup_type']
    search_fields = ['name']
    ordering_fields = ['created_at', 'run_at']
    ordering = ['-run_at']

    def get_queryset(self):
        user = self.request.user
        if user.is_superuser:
            return BackupOneOffTask.objects.all().select_related('instance', 'created_by', 'backup_record')
        user_teams = user.teams.all()
        return BackupOneOffTask.objects.filter(
            instance__team__in=user_teams
        ).select_related('instance', 'created_by', 'backup_record')

    def get_serializer_class(self):
        if self.action in ['create', 'update', 'partial_update']:
            return BackupOneOffTaskCreateSerializer
        return BackupOneOffTaskSerializer

    def get_permissions(self):
        if self.action in ['create', 'update', 'partial_update', 'destroy', 'run_now', 'cancel']:
            return [IsAuthenticated(), IsTeamAdmin()]
        return super().get_permissions()

    def perform_create(self, serializer):
        task = serializer.save(created_by=self.request.user)
        # 使用 ETA 调度
        try:
            execute_oneoff_backup_task = __import__('apps.backups.tasks', fromlist=['execute_oneoff_backup_task']).execute_oneoff_backup_task
            # 按计划时间调度一次性任务。
            async_result = execute_oneoff_backup_task.apply_async((task.id,), eta=task.run_at)
            task.task_id = async_result.id
            task.save(update_fields=['task_id'])
        except Exception as exc:
            logger.warning(f"定时任务调度失败: {exc}")

    @action(detail=True, methods=['post'], url_path='run-now')
    def run_now(self, request, pk=None):
        task = self.get_object()
        try:
            execute_oneoff_backup_task = __import__('apps.backups.tasks', fromlist=['execute_oneoff_backup_task']).execute_oneoff_backup_task
            # 忽略计划时间立即触发。
            async_result = execute_oneoff_backup_task.delay(task.id)
            task.task_id = async_result.id
            task.save(update_fields=['task_id'])
            return Response({'success': True, 'message': '任务已触发', 'task_id': async_result.id})
        except Exception as exc:
            logger.exception(f"立即执行失败: {exc}")
            return Response({'success': False, 'message': f'立即执行失败: {exc}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

    @action(detail=True, methods=['post'], url_path='cancel')
    def cancel(self, request, pk=None):
        task = self.get_object()
        if task.status not in ['pending', 'running']:
            return Response({'success': False, 'message': '任务状态不可取消'}, status=status.HTTP_400_BAD_REQUEST)
        task.status = 'canceled'
        task.save(update_fields=['status'])
        return Response({'success': True, 'message': '任务已取消'})


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
