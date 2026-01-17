from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('backups', '0006_backup_oneoff_task'),
    ]

    operations = [
        migrations.AddField(
            model_name='backupstrategy',
            name='store_local',
            field=models.BooleanField(default=True, help_text='将备份保存到本地存储路径', verbose_name='本地保存'),
        ),
        migrations.AddField(
            model_name='backupstrategy',
            name='store_remote',
            field=models.BooleanField(default=False, help_text='通过 SSH 保存到远程服务器目录', verbose_name='远程保存'),
        ),
        migrations.AddField(
            model_name='backupstrategy',
            name='store_oss',
            field=models.BooleanField(default=False, help_text='上传到对象存储（如 OSS）', verbose_name='云存储保存'),
        ),
        migrations.AddField(
            model_name='backupstrategy',
            name='remote_storage_path',
            field=models.CharField(blank=True, help_text='远程服务器存储路径（优先于实例的远程备份目录）', max_length=500, verbose_name='远程存储路径'),
        ),
        migrations.AddField(
            model_name='backuponeofftask',
            name='storage_path',
            field=models.CharField(blank=True, help_text='备份文件的存储路径，为空则使用默认路径', max_length=500, verbose_name='存储路径'),
        ),
        migrations.AddField(
            model_name='backuponeofftask',
            name='store_local',
            field=models.BooleanField(default=True, help_text='将备份保存到本地存储路径', verbose_name='本地保存'),
        ),
        migrations.AddField(
            model_name='backuponeofftask',
            name='store_remote',
            field=models.BooleanField(default=False, help_text='通过 SSH 保存到远程服务器目录', verbose_name='远程保存'),
        ),
        migrations.AddField(
            model_name='backuponeofftask',
            name='store_oss',
            field=models.BooleanField(default=False, help_text='上传到对象存储（如 OSS）', verbose_name='云存储保存'),
        ),
        migrations.AddField(
            model_name='backuponeofftask',
            name='remote_storage_path',
            field=models.CharField(blank=True, help_text='远程服务器存储路径（优先于实例的远程备份目录）', max_length=500, verbose_name='远程存储路径'),
        ),
    ]
