"""
Django settings initialization
Imports the appropriate settings module based on the environment
"""
import os
from decouple import config

# 从环境变量获取运行环境,默认为 development
ENVIRONMENT = config('DJANGO_ENV', default='development')

if ENVIRONMENT == 'production':
    from .production import *
else:
    from .development import *