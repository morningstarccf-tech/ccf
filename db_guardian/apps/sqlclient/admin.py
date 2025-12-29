"""
SQL客户端应用 Admin 配置
提供查询历史的后台管理界面
"""
from django.contrib import admin
from django.utils.html import format_html
from .models import QueryHistory


@admin.register(QueryHistory)
class QueryHistoryAdmin(admin.ModelAdmin):
    """SQL执行历史 Admin 配置"""
    
    list_display = [
        'id',
        'instance_alias',
        'database_name',
        'sql_type',
        'status_display',
        'rows_affected',
        'execution_time_ms',
        'executed_by_username',
        'executed_at'
    ]
    
    list_filter = [
        'sql_type',
        'status',
        'executed_at',
        'instance'
    ]
    
    search_fields = [
        'sql_statement',
        'database_name',
        'executed_by__username',
        'instance__alias'
    ]
    
    readonly_fields = [
        'instance',
        'database_name',
        'sql_statement',
        'sql_type',
        'status',
        'rows_affected',
        'execution_time_ms',
        'error_message',
        'result_cached',
        'result_cache_key',
        'executed_by',
        'executed_at'
    ]
    
    list_per_page = 50
    date_hierarchy = 'executed_at'
    ordering = ['-executed_at']
    
    fieldsets = [
        ('基本信息', {
            'fields': [
                'instance',
                'database_name',
                'executed_by',
                'executed_at'
            ]
        }),
        ('SQL 详情', {
            'fields': [
                'sql_statement',
                'sql_type',
                'status'
            ]
        }),
        ('执行结果', {
            'fields': [
                'rows_affected',
                'execution_time_ms',
                'error_message'
            ]
        }),
        ('缓存信息', {
            'fields': [
                'result_cached',
                'result_cache_key'
            ],
            'classes': ['collapse']
        })
    ]
    
    def instance_alias(self, obj):
        """显示实例别名"""
        return obj.instance.alias
    instance_alias.short_description = '实例'
    instance_alias.admin_order_field = 'instance__alias'
    
    def executed_by_username(self, obj):
        """显示执行者用户名"""
        if obj.executed_by:
            return obj.executed_by.username
        return '-'
    executed_by_username.short_description = '执行者'
    executed_by_username.admin_order_field = 'executed_by__username'
    
    def status_display(self, obj):
        """美化状态显示"""
        color_map = {
            'success': 'green',
            'failed': 'red',
            'timeout': 'orange'
        }
        color = color_map.get(obj.status, 'gray')
        return format_html(
            '<span style="color: {}; font-weight: bold;">{}</span>',
            color,
            obj.get_status_display()
        )
    status_display.short_description = '状态'
    status_display.admin_order_field = 'status'
    
    def has_add_permission(self, request):
        """禁止手动添加历史记录"""
        return False
    
    def has_change_permission(self, request, obj=None):
        """只读，不允许修改"""
        return False
    
    def has_delete_permission(self, request, obj=None):
        """允许删除历史记录"""
        return request.user.is_superuser
