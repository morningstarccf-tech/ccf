# DB-Guardian - 一体化MySQL数据库管理与备份平台

[![Python](https://img.shields.io/badge/Python-3.11-blue.svg)](https://www.python.org/)
[![Django](https://img.shields.io/badge/Django-4.2+-green.svg)](https://www.djangoproject.com/)
[![License](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

## 项目简介

DB-Guardian 是一个企业级的MySQL数据库管理和备份平台后端服务，提供自动化备份策略、精细化权限管控、实时监控和在线SQL客户端等功能。本项目采用前后端分离架构，基于Django和Django REST Framework构建。

## 核心功能

- **用户与权限中心 (RBAC)**：完善的角色权限管理体系
- **数据库实例管理**：统一管理多个MySQL实例
- **备份与恢复中心**：自动化备份策略和一键恢复
- **在线SQL管理**：Web化的SQL执行环境

## 技术栈

- **语言**：Python 3.11
- **Web框架**：Django 4.2+, Django REST Framework
- **异步任务**：Celery, Django Celery Beat
- **数据库**：PostgreSQL (应用数据)
- **消息队列**：Redis
- **依赖管理**：uv

## 快速开始

### 环境要求

- Python 3.11
- PostgreSQL 12+
- Redis 6+
- uv (依赖管理工具)

### 安装步骤

1. **克隆项目**

```bash
git clone <repository-url>
cd db_guardian
```

2. **安装依赖**

```bash
uv sync
```

3. **配置环境变量**

```bash
cp .env.example .env
# 编辑 .env 文件，配置数据库连接等信息
```

4. **初始化数据库**

```bash
uv run python manage.py migrate
```

5. **创建超级管理员**

```bash
uv run python manage.py createsuperuser
```

6. **启动开发服务器**

```bash
uv run python manage.py runserver
```

7. **启动 Celery Worker (另开终端)**

```bash
uv run celery -A tasks worker -l info
```

8. **启动 Celery Beat (另开终端)**

```bash
uv run celery -A tasks beat -l info
```

## Docker 部署

```bash
docker-compose up -d
```

## 项目结构

```
db_guardian/
├── config/              # Django配置
│   ├── settings/        # 分环境配置
│   │   ├── base.py      # 基础配置
│   │   ├── development.py
│   │   └── production.py
│   ├── urls.py
│   ├── wsgi.py
│   └── asgi.py
├── apps/                # 应用模块
│   ├── authentication/  # 用户认证
│   ├── instances/       # 实例管理
│   ├── backups/         # 备份恢复
│   └── sqlclient/       # SQL客户端
├── common/              # 公共工具
├── tasks/               # Celery任务
│   ├── celery.py        # Celery配置
│   └── __init__.py
├── logs/                # 日志目录
├── manage.py
└── pyproject.toml       # 项目依赖
```

## API 文档

启动服务后访问：

- Swagger UI: `http://localhost:8000/api/docs/`
- ReDoc: `http://localhost:8000/api/redoc/`

## 开发指南

### 添加新依赖

```bash
uv add <package-name>
```

### 创建新的Django应用

```bash
uv run python manage.py startapp <app-name> apps/<app-name>
```

### 运行测试

```bash
uv run python manage.py test
```

### 代码格式化

```bash
uv run black .
uv run isort .
```

## 环境变量说明

| 变量名              | 说明                              | 默认值                   |
| ------------------- | --------------------------------- | ------------------------ |
| `DJANGO_ENV`        | 运行环境 (development/production) | development              |
| `SECRET_KEY`        | Django密钥                        | -                        |
| `DEBUG`             | 调试模式                          | False                    |
| `DB_NAME`           | 数据库名                          | db_guardian              |
| `DB_USER`           | 数据库用户                        | postgres                 |
| `DB_PASSWORD`       | 数据库密码                        | -                        |
| `CELERY_BROKER_URL` | Celery消息代理                    | redis://localhost:6379/0 |

详见 `.env.example` 文件。

## 贡献指南

欢迎提交Issue和Pull Request。

## 许可证

MIT License

## 联系方式

- 项目主页：<repository-url>
- 问题反馈：<issues-url>

## 版本历史

- v0.1.0 - 项目初始化
  - 完成项目基础架构搭建
  - 配置Django + Celery环境
  - 实现分环境配置管理