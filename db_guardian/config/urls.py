"""
AuroraVault 主路由配置
定义项目所有应用的URL路由
"""
from django.contrib import admin
from django.urls import path, include
from django.views.generic import RedirectView
from django.conf import settings
from django.conf.urls.static import static

urlpatterns = [
    # Django Admin
    path('admin/', admin.site.urls),
    # 根路径重定向到 admin（使首页更友好）
    path('', RedirectView.as_view(url='/admin/', permanent=False)),
    
    # API 路由
    path('api/auth/', include('apps.authentication.urls')),
    path('api/instances/', include('apps.instances.urls')),
    path('api/backups/', include('apps.backups.urls')),
    path('api/sql/', include('apps.sqlclient.urls')),
]

# 开发环境下的静态文件和媒体文件服务
if settings.DEBUG:
    urlpatterns += static(settings.STATIC_URL, document_root=settings.STATIC_ROOT)
    urlpatterns += static(settings.MEDIA_URL, document_root=settings.MEDIA_ROOT)
