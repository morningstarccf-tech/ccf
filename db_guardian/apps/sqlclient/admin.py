"""
SQL客户端应用 Admin 配置
提供查询历史的后台管理界面
"""
from django.contrib import admin, messages
from django import forms
from django.template.response import TemplateResponse
from django.urls import reverse, path
from django.utils.html import format_html
from django.utils.safestring import mark_safe
from .models import QueryHistory
from .services import QueryExecutor
from apps.instances.models import MySQLInstance


class QueryExecutionForm(forms.Form):
    """SQL 执行表单"""

    instance = forms.ModelChoiceField(
        queryset=MySQLInstance.objects.all(),
        label='MySQL 实例'
    )
    database = forms.CharField(
        required=False,
        label='数据库名称'
    )
    sql = forms.CharField(
        widget=forms.Textarea(attrs={'rows': 8}),
        label='SQL 语句'
    )
    timeout = forms.IntegerField(
        initial=30,
        min_value=1,
        max_value=3600,
        label='超时(秒)'
    )
    apply_limit = forms.BooleanField(
        required=False,
        initial=True,
        label='自动限制行数'
    )
    max_rows = forms.IntegerField(
        initial=1000,
        min_value=1,
        max_value=100000,
        label='最大返回行数'
    )


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
    change_list_template = 'admin/sqlclient/queryhistory/change_list.html'
    
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

    def get_urls(self):
        """添加 SQL 执行页面"""
        urls = super().get_urls()
        custom_urls = [
            path(
                'execute/',
                self.admin_site.admin_view(self.execute_sql_view),
                name='sqlclient_queryhistory_execute'
            ),
        ]
        return custom_urls + urls

    def execute_sql_view(self, request):
        """执行 SQL 并写入历史记录"""
        result = None
        if request.method == 'POST':
            form = QueryExecutionForm(request.POST)
            if form.is_valid():
                instance = form.cleaned_data['instance']
                database = form.cleaned_data.get('database') or None
                sql = form.cleaned_data['sql']
                timeout = form.cleaned_data['timeout']
                apply_limit = form.cleaned_data['apply_limit']
                max_rows = form.cleaned_data['max_rows']

                executor = QueryExecutor(instance, request.user)
                result = executor.execute_query(
                    sql=sql,
                    database=database,
                    timeout=timeout,
                    apply_limit=apply_limit,
                    max_rows=max_rows
                )

                if result.get('success'):
                    history_id = result.get('history_id')
                    if history_id:
                        history_url = reverse('admin:sqlclient_queryhistory_change', args=[history_id])
                        messages.success(
                            request,
                            mark_safe(f'执行成功，<a href="{history_url}">查看历史记录</a>')
                        )
                    else:
                        messages.success(request, '执行成功')
                else:
                    messages.error(request, result.get('message', '执行失败'))
        else:
            form = QueryExecutionForm()

        context = {
            **self.admin_site.each_context(request),
            'title': 'SQL 执行',
            'form': form,
            'result': result,
        }
        return TemplateResponse(request, 'admin/sqlclient/queryhistory/execute.html', context)
