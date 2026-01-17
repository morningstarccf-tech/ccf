from django.conf import settings
from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('backups', '0005_default_backup_paths'),
        ('instances', '0003_add_remote_backup_root'),
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
    ]

    operations = [
        migrations.CreateModel(
            name='BackupOneOffTask',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('name', models.CharField(help_text='一次性定时任务名称', max_length=120, verbose_name='任务名称')),
                ('databases', models.JSONField(blank=True, help_text='要备份的数据库列表，为空表示备份所有数据库', null=True, verbose_name='数据库列表')),
                ('backup_type', models.CharField(choices=[('full', '全量备份'), ('incremental', '增量备份'), ('hot', '热备份'), ('cold', '冷备份')], default='full', help_text='备份类型：全量、增量、热备或冷备', max_length=20, verbose_name='备份类型')),
                ('run_at', models.DateTimeField(help_text='任务计划执行时间', verbose_name='执行时间')),
                ('compress', models.BooleanField(default=True, help_text='是否压缩备份文件', verbose_name='是否压缩')),
                ('status', models.CharField(choices=[('pending', '等待中'), ('running', '执行中'), ('success', '成功'), ('failed', '失败'), ('canceled', '已取消')], default='pending', help_text='任务执行状态', max_length=20, verbose_name='状态')),
                ('task_id', models.CharField(blank=True, help_text='Celery 调度的任务ID', max_length=100, verbose_name='Celery 任务ID')),
                ('error_message', models.TextField(blank=True, help_text='失败原因', verbose_name='错误信息')),
                ('created_at', models.DateTimeField(auto_now_add=True, verbose_name='创建时间')),
                ('started_at', models.DateTimeField(blank=True, null=True, verbose_name='开始时间')),
                ('finished_at', models.DateTimeField(blank=True, null=True, verbose_name='结束时间')),
                ('backup_record', models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.SET_NULL, related_name='oneoff_tasks', to='backups.backuprecord', verbose_name='关联备份记录')),
                ('created_by', models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.SET_NULL, related_name='created_oneoff_tasks', to=settings.AUTH_USER_MODEL, verbose_name='创建者')),
                ('instance', models.ForeignKey(help_text='要备份的 MySQL 实例', on_delete=django.db.models.deletion.CASCADE, related_name='oneoff_backup_tasks', to='instances.mysqlinstance', verbose_name='MySQL 实例')),
            ],
            options={
                'verbose_name': '定时任务',
                'verbose_name_plural': '定时任务',
                'db_table': 'backup_oneoff_task',
                'ordering': ['-run_at'],
            },
        ),
        migrations.AddIndex(
            model_name='backuponeofftask',
            index=models.Index(fields=['status'], name='idx_oneoff_status'),
        ),
        migrations.AddIndex(
            model_name='backuponeofftask',
            index=models.Index(fields=['instance', 'run_at'], name='idx_oneoff_instance_time'),
        ),
    ]
