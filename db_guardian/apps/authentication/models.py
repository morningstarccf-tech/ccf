"""
认证应用的数据库模型

包含用户、角色、权限、团队等核心模型，实现 RBAC 权限系统。
"""
from django.db import models
from django.contrib.auth.models import AbstractUser
from django.core.validators import RegexValidator
from django.utils.translation import gettext_lazy as _


class User(AbstractUser):
    """
    自定义用户模型，扩展 Django 默认用户模型
    
    字段说明：
    - username: 用户名（继承自 AbstractUser，唯一）
    - email: 邮箱（继承自 AbstractUser，唯一）
    - password: 密码（继承自 AbstractUser，加密存储）
    - first_name, last_name: 姓名（继承自 AbstractUser）
    - is_active: 是否激活（继承自 AbstractUser）
    - is_staff: 是否员工（继承自 AbstractUser）
    - is_superuser: 是否超级用户（继承自 AbstractUser）
    - date_joined: 加入时间（继承自 AbstractUser）
    - phone: 手机号（可选）
    - avatar: 头像 URL（可选）
    - created_at: 创建时间（自动）
    - updated_at: 更新时间（自动）
    """
    
    phone_validator = RegexValidator(
        regex=r'^1[3-9]\d{9}$',
        message=_('请输入有效的手机号码')
    )
    
    phone = models.CharField(
        _('手机号'),
        max_length=11,
        blank=True,
        null=True,
        unique=True,
        validators=[phone_validator],
        help_text=_('11位手机号码')
    )
    
    avatar = models.URLField(
        _('头像'),
        max_length=500,
        blank=True,
        null=True,
        help_text=_('用户头像的 URL 地址')
    )
    
    created_at = models.DateTimeField(
        _('创建时间'),
        auto_now_add=True,
        help_text=_('用户账户创建时间')
    )
    
    updated_at = models.DateTimeField(
        _('更新时间'),
        auto_now=True,
        help_text=_('用户信息最后更新时间')
    )
    
    class Meta:
        db_table = 'auth_user'
        verbose_name = _('用户')
        verbose_name_plural = _('用户')
        ordering = ['-created_at']
        indexes = [
            models.Index(fields=['username'], name='idx_user_username'),
            models.Index(fields=['email'], name='idx_user_email'),
            models.Index(fields=['phone'], name='idx_user_phone'),
            models.Index(fields=['is_active'], name='idx_user_active'),
        ]
    
    def __str__(self):
        return f"{self.username} ({self.email})"
    
    def get_team_roles(self):
        """
        获取用户在所有团队中的角色
        
        Returns:
            QuerySet: 包含团队和角色信息的 TeamMember 查询集
        """
        return self.team_memberships.select_related('team', 'role')
    
    def has_team_permission(self, team, permission_slug):
        """
        检查用户在指定团队是否拥有某个权限
        
        Args:
            team: Team 实例或团队 ID
            permission_slug: 权限标识符
            
        Returns:
            bool: 是否拥有该权限
        """
        if self.is_superuser:
            return True
        
        team_id = team.id if isinstance(team, Team) else team
        membership = self.team_memberships.filter(
            team_id=team_id
        ).select_related('role').first()
        
        if not membership:
            return False
        
        return membership.role.permissions.filter(slug=permission_slug).exists()


class Permission(models.Model):
    """
    权限模型
    
    定义系统中的所有权限点，支持分类管理。
    权限通过 slug 唯一标识，用于代码中的权限检查。
    """
    
    CATEGORY_CHOICES = [
        ('user', _('用户管理')),
        ('team', _('团队管理')),
        ('instance', _('实例管理')),
        ('backup', _('备份管理')),
        ('sql', _('SQL执行')),
        ('monitoring', _('监控管理')),
        ('system', _('系统管理')),
    ]
    
    name = models.CharField(
        _('权限名称'),
        max_length=100,
        help_text=_('权限的显示名称，如"查看实例"')
    )
    
    slug = models.SlugField(
        _('权限标识'),
        max_length=100,
        unique=True,
        help_text=_('权限的唯一标识符，如"view_instance"')
    )
    
    category = models.CharField(
        _('权限分类'),
        max_length=20,
        choices=CATEGORY_CHOICES,
        default='system',
        help_text=_('权限所属的功能分类')
    )
    
    description = models.TextField(
        _('权限描述'),
        blank=True,
        help_text=_('详细说明该权限的作用和范围')
    )
    
    created_at = models.DateTimeField(
        _('创建时间'),
        auto_now_add=True
    )
    
    class Meta:
        db_table = 'auth_custom_permission'
        verbose_name = _('权限')
        verbose_name_plural = _('权限')
        ordering = ['category', 'slug']
        indexes = [
            models.Index(fields=['slug'], name='idx_permission_slug'),
            models.Index(fields=['category'], name='idx_permission_category'),
        ]
    
    def __str__(self):
        return f"{self.get_category_display()} - {self.name}"


class Role(models.Model):
    """
    角色模型
    
    定义系统中的角色，每个角色关联多个权限。
    内置角色（is_builtin=True）不允许删除，只能修改权限配置。
    """
    
    name = models.CharField(
        _('角色名称'),
        max_length=50,
        unique=True,
        help_text=_('角色的显示名称，如"超级管理员"')
    )
    
    slug = models.SlugField(
        _('角色标识'),
        max_length=50,
        unique=True,
        help_text=_('角色的唯一标识符，如"super_admin"')
    )
    
    description = models.TextField(
        _('角色描述'),
        blank=True,
        help_text=_('详细说明该角色的职责和权限范围')
    )
    
    is_builtin = models.BooleanField(
        _('内置角色'),
        default=False,
        help_text=_('内置角色不可删除，只能修改权限')
    )
    
    permissions = models.ManyToManyField(
        Permission,
        related_name='roles',
        verbose_name=_('权限'),
        blank=True,
        help_text=_('该角色拥有的所有权限')
    )
    
    created_at = models.DateTimeField(
        _('创建时间'),
        auto_now_add=True
    )
    
    class Meta:
        db_table = 'auth_role'
        verbose_name = _('角色')
        verbose_name_plural = _('角色')
        ordering = ['-is_builtin', 'slug']
        indexes = [
            models.Index(fields=['slug'], name='idx_role_slug'),
            models.Index(fields=['is_builtin'], name='idx_role_builtin'),
        ]
    
    def __str__(self):
        return self.name
    
    def has_permission(self, permission_slug):
        """
        检查角色是否拥有指定权限
        
        Args:
            permission_slug: 权限标识符
            
        Returns:
            bool: 是否拥有该权限
        """
        return self.permissions.filter(slug=permission_slug).exists()
    
    def get_permissions_by_category(self):
        """
        按分类获取角色的所有权限
        
        Returns:
            dict: 以分类为 key，权限列表为 value 的字典
        """
        permissions = self.permissions.all().order_by('category', 'slug')
        result = {}
        for perm in permissions:
            if perm.category not in result:
                result[perm.category] = []
            result[perm.category].append(perm)
        return result


class Team(models.Model):
    """
    团队模型
    
    组织用户的协作单元，每个团队有一个所有者和多个成员。
    团队用于隔离资源和权限范围。
    """
    
    name = models.CharField(
        _('团队名称'),
        max_length=100,
        unique=True,
        help_text=_('团队的显示名称')
    )
    
    description = models.TextField(
        _('团队描述'),
        blank=True,
        help_text=_('团队的简介和用途说明')
    )
    
    owner = models.ForeignKey(
        User,
        on_delete=models.PROTECT,
        related_name='owned_teams',
        verbose_name=_('团队所有者'),
        help_text=_('创建并拥有该团队的用户，不能删除')
    )
    
    members = models.ManyToManyField(
        User,
        through='TeamMember',
        related_name='teams',
        verbose_name=_('团队成员'),
        help_text=_('团队的所有成员用户')
    )
    
    created_at = models.DateTimeField(
        _('创建时间'),
        auto_now_add=True
    )
    
    updated_at = models.DateTimeField(
        _('更新时间'),
        auto_now=True
    )
    
    class Meta:
        db_table = 'auth_team'
        verbose_name = _('团队')
        verbose_name_plural = _('团队')
        ordering = ['-created_at']
        indexes = [
            models.Index(fields=['name'], name='idx_team_name'),
            models.Index(fields=['owner'], name='idx_team_owner'),
        ]
    
    def __str__(self):
        return f"{self.name} (所有者: {self.owner.username})"
    
    def add_member(self, user, role):
        """
        添加团队成员
        
        Args:
            user: User 实例
            role: Role 实例
            
        Returns:
            TeamMember: 创建的团队成员关系
        """
        return TeamMember.objects.create(
            team=self,
            user=user,
            role=role
        )
    
    def remove_member(self, user):
        """
        移除团队成员
        
        Args:
            user: User 实例或用户 ID
        """
        user_id = user.id if isinstance(user, User) else user
        TeamMember.objects.filter(team=self, user_id=user_id).delete()
    
    def get_member_role(self, user):
        """
        获取成员在团队中的角色
        
        Args:
            user: User 实例或用户 ID
            
        Returns:
            Role: 角色实例，如果不是成员则返回 None
        """
        user_id = user.id if isinstance(user, User) else user
        membership = TeamMember.objects.filter(
            team=self,
            user_id=user_id
        ).select_related('role').first()
        return membership.role if membership else None


class TeamMember(models.Model):
    """
    团队成员关系模型
    
    多对多中间表，连接用户和团队，并记录用户在团队中的角色。
    一个用户在一个团队中只能有一个角色。
    """
    
    user = models.ForeignKey(
        User,
        on_delete=models.CASCADE,
        related_name='team_memberships',
        verbose_name=_('用户')
    )
    
    team = models.ForeignKey(
        Team,
        on_delete=models.CASCADE,
        related_name='memberships',
        verbose_name=_('团队')
    )
    
    role = models.ForeignKey(
        Role,
        on_delete=models.PROTECT,
        related_name='team_members',
        verbose_name=_('角色'),
        help_text=_('用户在该团队中的角色')
    )
    
    joined_at = models.DateTimeField(
        _('加入时间'),
        auto_now_add=True,
        help_text=_('用户加入团队的时间')
    )
    
    class Meta:
        db_table = 'auth_team_member'
        verbose_name = _('团队成员')
        verbose_name_plural = _('团队成员')
        ordering = ['-joined_at']
        unique_together = [['user', 'team']]
        indexes = [
            models.Index(fields=['user', 'team'], name='idx_tm_user_team'),
            models.Index(fields=['team', 'role'], name='idx_tm_team_role'),
        ]
    
    def __str__(self):
        return f"{self.user.username} @ {self.team.name} ({self.role.name})"
    
    def has_permission(self, permission_slug):
        """
        检查成员是否拥有指定权限
        
        Args:
            permission_slug: 权限标识符
            
        Returns:
            bool: 是否拥有该权限
        """
        if self.user.is_superuser:
            return True
        return self.role.has_permission(permission_slug)
