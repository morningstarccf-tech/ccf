from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('backups', '0003_alter_backuprecord_backup_type_and_more'),
    ]

    operations = [
        migrations.AddField(
            model_name='backuprecord',
            name='remote_path',
            field=models.CharField(
                blank=True,
                max_length=500,
                verbose_name='远程路径',
                help_text='远程服务器备份路径',
                default='',
            ),
            preserve_default=False,
        ),
        migrations.AddField(
            model_name='backuprecord',
            name='object_storage_path',
            field=models.CharField(
                blank=True,
                max_length=500,
                verbose_name='对象存储路径',
                help_text='对象存储路径（如 OSS）',
                default='',
            ),
            preserve_default=False,
        ),
    ]
