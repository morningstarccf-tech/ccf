"""
Production settings for DB-Guardian project.
生产环境的安全配置
"""
from .base import *

# Debug mode - 生产环境必须关闭
DEBUG = False

# Allowed hosts - 必须从环境变量配置
ALLOWED_HOSTS = config(
    'ALLOWED_HOSTS',
    default='',
    cast=lambda v: [s.strip() for s in v.split(',') if s.strip()]
)

# Security settings
SECURE_SSL_REDIRECT = config('SECURE_SSL_REDIRECT', default=True, cast=bool)
SECURE_PROXY_SSL_HEADER = ('HTTP_X_FORWARDED_PROTO', 'https')
SECURE_HSTS_SECONDS = 31536000  # 1 year
SECURE_HSTS_INCLUDE_SUBDOMAINS = True
SECURE_HSTS_PRELOAD = True
SECURE_BROWSER_XSS_FILTER = True
SECURE_CONTENT_TYPE_NOSNIFF = True

# Session security
SESSION_COOKIE_SECURE = True
SESSION_COOKIE_HTTPONLY = True
SESSION_COOKIE_SAMESITE = 'Lax'

# CSRF security
CSRF_COOKIE_SECURE = True
CSRF_COOKIE_HTTPONLY = True
CSRF_COOKIE_SAMESITE = 'Lax'

# Database - 生产环境必须使用 PostgreSQL
DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.postgresql',
        'NAME': config('DB_NAME'),
        'USER': config('DB_USER'),
        'PASSWORD': config('DB_PASSWORD'),
        'HOST': config('DB_HOST'),
        'PORT': config('DB_PORT', default='5432'),
        'ATOMIC_REQUESTS': True,
        'CONN_MAX_AGE': 600,
        'OPTIONS': {
            'connect_timeout': 10,
        },
    }
}

# Encryption key - 生产环境必须显式配置
if not config('ENCRYPTION_KEY', default='').strip():
    raise ValueError('ENCRYPTION_KEY must be set in production')
ENCRYPTION_KEY = config('ENCRYPTION_KEY').strip()

# Cache - 生产环境使用 Redis
CACHES = {
    'default': {
        'BACKEND': 'django.core.cache.backends.redis.RedisCache',
        'LOCATION': config('REDIS_CACHE_URL', default='redis://localhost:6379/1'),
        'OPTIONS': {
            'CLIENT_CLASS': 'django_redis.client.DefaultClient',
        },
        'KEY_PREFIX': 'db_guardian',
        'TIMEOUT': 300,
    }
}

# Email backend for production
EMAIL_BACKEND = 'django.core.mail.backends.smtp.EmailBackend'
EMAIL_HOST = config('EMAIL_HOST', default='smtp.gmail.com')
EMAIL_PORT = config('EMAIL_PORT', default=587, cast=int)
EMAIL_USE_TLS = config('EMAIL_USE_TLS', default=True, cast=bool)
EMAIL_HOST_USER = config('EMAIL_HOST_USER', default='')
EMAIL_HOST_PASSWORD = config('EMAIL_HOST_PASSWORD', default='')
DEFAULT_FROM_EMAIL = config('DEFAULT_FROM_EMAIL', default='noreply@db-guardian.com')

# Logging - 生产环境日志配置
from pathlib import Path
LOG_FILE_PATH = Path(
    config('LOG_FILE_PATH', default=str(BASE_DIR / 'logs' / 'db_guardian.log'))
)
LOG_FILE_PATH.parent.mkdir(parents=True, exist_ok=True)
LOGGING['handlers']['file']['filename'] = str(LOG_FILE_PATH)
LOGGING['root']['level'] = 'INFO'
LOGGING['loggers']['apps']['level'] = 'INFO'

# Static files - 生产环境使用 WhiteNoise 或 云存储
STATIC_ROOT = Path(
    config('STATIC_ROOT', default=str(BASE_DIR / 'staticfiles'))
)
MEDIA_ROOT = Path(
    config('MEDIA_ROOT', default=str(BASE_DIR / 'media'))
)

# 如果使用 WhiteNoise (推荐用于 Docker 部署)
MIDDLEWARE.insert(1, 'whitenoise.middleware.WhiteNoiseMiddleware')
STATICFILES_STORAGE = 'whitenoise.storage.CompressedManifestStaticFilesStorage'

# Celery settings for production
CELERY_BROKER_CONNECTION_RETRY_ON_STARTUP = True
CELERY_BROKER_CONNECTION_MAX_RETRIES = 10
