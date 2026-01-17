"""
Celery tasks and background job configurations for AuroraVault
"""
from .celery import app as celery_app

__all__ = ('celery_app',)
