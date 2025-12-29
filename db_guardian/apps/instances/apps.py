from django.apps import AppConfig


class InstancesConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'apps.instances'
    verbose_name = 'MySQL实例管理'
