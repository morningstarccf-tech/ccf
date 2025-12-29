from django.apps import AppConfig


class SqlclientConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'apps.sqlclient'
    verbose_name = 'SQL客户端'
