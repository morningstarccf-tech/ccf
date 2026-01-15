# DB-Guardian 项目说明

## 项目简介
DB-Guardian 是一个一体化 MySQL 数据库管理与备份平台的后端服务，基于 Django + DRF + Celery 构建，提供 RBAC 权限、实例管理、备份恢复、SQL 客户端等能力。

需求与设计文档见：`基于Python的MySQL数据库备份恢复方案设计与实现.md`。

## 目录结构
- `db_guardian/`: 后端服务主体
- `基于Python的MySQL数据库备份恢复方案设计与实现.md`: 需求规格说明

`db_guardian/` 内部主要结构：
- `config/`: Django 配置（分环境）
- `apps/`: 业务模块
  - `authentication/`: 用户、角色、团队、权限（RBAC）
  - `instances/`: MySQL 实例、数据库与监控
  - `backups/`: 备份策略与备份记录
  - `sqlclient/`: SQL 执行、历史与结果导出
- `tasks/`: Celery 配置与定时任务
- `Dockerfile`/`docker-compose.yml`: 容器化部署

## 运行方式

### 开发模式（本地）
1. 安装依赖
   - 推荐使用 `uv` 管理依赖
2. 配置环境变量
   - 复制并编辑 `db_guardian/.env.example` 为 `db_guardian/.env`（或设置系统环境变量）
3. 初始化数据库
   - `uv run python manage.py migrate`
4. 创建超级管理员
   - `uv run python manage.py createsuperuser`
5. 启动服务
   - `uv run python manage.py runserver`
6. 启动 Celery Worker/Beat（用于状态刷新、备份调度等）
   - `uv run celery -A tasks worker -l info`
   - `uv run celery -A tasks beat -l info`

### Docker 部署
- 在 `db_guardian/` 目录运行：
  - `docker-compose up -d`
  - 首次运行前确保已配置 `db_guardian/.env`

## 关键环境变量
- `DJANGO_ENV`: 运行环境（development/production）
- `SECRET_KEY`: Django 密钥
- `ENCRYPTION_KEY`: MySQL 实例密码加密密钥（生产环境必须显式配置）
- `DB_HOST`/`DB_PORT`/`DB_NAME`/`DB_USER`/`DB_PASSWORD`: PostgreSQL 连接
- `CELERY_BROKER_URL`/`CELERY_RESULT_BACKEND`: Celery 连接
- `REDIS_CACHE_URL`: Redis 缓存
- `INSTANCE_STATUS_STALE_SECONDS`: 实例状态过期阈值（秒）

## 实例状态刷新说明
- 实例状态会在列表/详情请求时检查是否过期，过期会触发一次健康检查。
- 也可调用 `/api/instances/{id}/refresh-status/` 立即刷新。
- 若未启动 Celery，状态只会在请求时刷新，不会自动定时更新。

## 备份类型说明
- 全量备份：使用 `mysqldump` 进行逻辑备份（可选压缩）
- 增量备份：使用 `xtrabackup` 做物理增量备份（需基准备份）
- 热备份：使用 `xtrabackup` 做物理全量备份（不停库）
- 冷备份：停止 MySQL 服务或容器后复制数据目录

### 热备/冷备/增量备份前置条件
- 实例必须配置 `data_dir`（MySQL 数据目录）
- 实例必须配置 SSH 连接信息（`ssh_host` / `ssh_user`，可选 `ssh_password` 或 `ssh_key_path`）
- 冷备份需配置部署方式：
  - Docker：填写 `docker_container_name`
  - systemd：填写 `mysql_service_name`
- 目标主机需安装 `xtrabackup`（用于热备/增量）

## 安全提示
- 开发环境未配置 `ENCRYPTION_KEY` 时会自动生成并写入 `db_guardian/.encryption_key`，请勿随意删除。
- 生产环境必须显式设置 `ENCRYPTION_KEY`，否则服务会拒绝启动。
