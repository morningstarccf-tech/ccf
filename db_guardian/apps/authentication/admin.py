"""
认证应用的 Django Admin 配置

为用户、角色、权限、团队等模型提供后台管理界面。
"""
from django.contrib import admin
from django.contrib.auth.admin import UserAdmin as BaseUserAdmin
from django.utils.translation import gettext_lazy as _
from .models import User, Role, Permission, Team, TeamMember


@admin.register(User)
class UserAdmin(BaseUserAdmin):
    """用户模型的 Admin 配置"""
    
    # 列表页显示字段
    list_display = ['username', 'email', 'phone', 'is_active', 'is_staff', 'created_at']
    list_filter = ['is_active', 'is_staff', 'is_superuser', 'created_at']
    search_fields = ['username', 'email', 'phone', 'first_name', 'last_name']
    ordering = ['-created_at']
    
    # 详情页字段分组
    fieldsets = (
        (None, {'fields': ('username', 'password')}),
        (_('个人信息'), {'fields': ('first_name', 'last_name', 'email', 'phone', 'avatar')}),
        (_('权限'), {
            'fields': ('is_active', 'is_staff', 'is_superuser', 'groups', 'user_permissions'),
        }),
        (_('重要日期'), {'fields': ('last_login', 'date_joined', 'created_at', 'updated_at')}),
    )
    
    # 添加用户页面字段
    add_fieldsets = (
        (None, {
            'classes': ('wide',),
            'fields': ('username', 'email', 'password1', 'password2', 'phone'),
        }),
    )
    
    readonly_fields = ['created_at', 'updated_at', 'date_joined', 'last_login']
    
    # 每页显示数量
    list_per_page = 20


@admin.register(Permission)
class PermissionAdmin(admin.ModelAdmin):
    """权限模型的 Admin 配置"""
    
    list_display = ['name', 'slug', 'category', 'created_at']
    list_filter = ['category', 'created_at']
    search_fields = ['name', 'slug', 'description']
    ordering = ['category', 'slug']
    
    fieldsets = (
        (_('基本信息'), {
            'fields': ('name', 'slug', 'category')
        }),
        (_('描述'), {
            'fields': ('description',)
        }),
        (_('元数据'), {
            'fields': ('created_at',),
            'classes': ('collapse',)
        }),
    )
    
    readonly_fields = ['created_at']
    list_per_page = 30


class RolePermissionInline(admin.TabularInline):
    """角色权限内联编辑"""
    model = Role.permissions.through
    extra = 1
    verbose_name = _('权限')
    verbose_name_plural = _('权限列表')


@admin.register(Role)
class RoleAdmin(admin.ModelAdmin):
    """角色模型的 Admin 配置"""
    
    list_display = ['name', 'slug', 'is_builtin', 'permission_count', 'created_at']
    list_filter = ['is_builtin', 'created_at']
    search_fields = ['name', 'slug', 'description']
    ordering = ['-is_builtin', 'slug']
    
    fieldsets = (
        (_('基本信息'), {
            'fields': ('name', 'slug', 'is_builtin')
        }),
        (_('描述'), {
            'fields': ('description',)
        }),
        (_('权限管理'), {
            'fields': ('permissions',),
            'description': _('为该角色分配权限。可以使用 Ctrl/Cmd 键多选。')
        }),
        (_('元数据'), {
            'fields': ('created_at',),
            'classes': ('collapse',)
        }),
    )
    
    filter_horizontal = ['permissions']
    readonly_fields = ['created_at']
    
    def permission_count(self, obj):
        """显示权限数量"""
        return obj.permissions.count()
    permission_count.short_description = _('权限数量')
    
    def get_readonly_fields(self, request, obj=None):
        """内置角色的 slug 和 is_builtin 字段只读"""
        readonly = list(super().get_readonly_fields(request, obj))
        if obj and obj.is_builtin:
            readonly.extend(['slug', 'is_builtin'])
        return readonly
    
    def has_delete_permission(self, request, obj=None):
        """内置角色不允许删除"""
        if obj and obj.is_builtin:
            return False
        return super().has_delete_permission(request, obj)


class TeamMemberInline(admin.TabularInline):
    """团队成员内联编辑"""
    model = TeamMember
    extra = 1
    autocomplete_fields = ['user', 'role']
    verbose_name = _('团队成员')
    verbose_name_plural = _('团队成员列表')
    readonly_fields = ['joined_at']


@admin.register(Team)
class TeamAdmin(admin.ModelAdmin):
    """团队模型的 Admin 配置"""
    
    list_display = ['name', 'owner', 'member_count', 'created_at', 'updated_at']
    list_filter = ['created_at', 'updated_at']
    search_fields = ['name', 'description', 'owner__username']
    ordering = ['-created_at']
    autocomplete_fields = ['owner']
    inlines = [TeamMemberInline]
    
    fieldsets = (
        (_('基本信息'), {
            'fields': ('name', 'owner')
        }),
        (_('描述'), {
            'fields': ('description',)
        }),
        (_('时间信息'), {
            'fields': ('created_at', 'updated_at'),
            'classes': ('collapse',)
        }),
    )
    
    readonly_fields = ['created_at', 'updated_at']
    
    def member_count(self, obj):
        """显示成员数量"""
        return obj.memberships.count()
    member_count.short_description = _('成员数量')
    
    list_per_page = 20


@admin.register(TeamMember)
class TeamMemberAdmin(admin.ModelAdmin):
    """团队成员模型的 Admin 配置"""
    
    list_display = ['user', 'team', 'role', 'joined_at']
    list_filter = ['role', 'joined_at']
    search_fields = ['user__username', 'user__email', 'team__name']
    ordering = ['-joined_at']
    autocomplete_fields = ['user', 'team', 'role']
    
    fieldsets = (
        (_('成员信息'), {
            'fields': ('user', 'team', 'role')
        }),
        (_('时间信息'), {
            'fields': ('joined_at',),
            'classes': ('collapse',)
        }),
    )
    
    readonly_fields = ['joined_at']
    list_per_page = 30


# 自定义 Admin 站点标题
admin.site.site_header = _('DB-Guardian 管理后台')
admin.site.site_title = _('DB-Guardian')
admin.site.index_title = _('欢迎使用 DB-Guardian 管理系统')
