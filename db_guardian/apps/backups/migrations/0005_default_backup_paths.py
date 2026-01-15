from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('backups', '0004_add_remote_storage_paths'),
    ]

    operations = [
        migrations.AlterField(
            model_name='backuprecord',
            name='remote_path',
            field=models.CharField(
                blank=True,
                default='',
                max_length=500,
                verbose_name='远程路径',
                help_text='远程服务器备份路径',
            ),
        ),
        migrations.AlterField(
            model_name='backuprecord',
            name='object_storage_path',
            field=models.CharField(
                blank=True,
                default='',
                max_length=500,
                verbose_name='对象存储路径',
                help_text='对象存储路径（如 OSS）',
            ),
        ),
    ]
