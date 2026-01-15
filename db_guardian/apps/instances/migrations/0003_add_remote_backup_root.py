from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('instances', '0002_backup_fields'),
    ]

    operations = [
        migrations.AddField(
            model_name='mysqlinstance',
            name='remote_backup_root',
            field=models.CharField(
                blank=True,
                max_length=500,
                verbose_name='远程备份目录',
                help_text='备份在 MySQL 服务器保留的目录（需配置 SSH）',
                default='',
            ),
            preserve_default=False,
        ),
    ]
