from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('backups', '0007_add_storage_targets'),
    ]

    operations = [
        migrations.AddField(
            model_name='backupstrategy',
            name='storage_mode',
            field=models.CharField(
                choices=[
                    ('default', '默认容器路径'),
                    ('mysql_host', 'MySQL 服务器路径'),
                    ('remote_server', '远程服务器路径'),
                    ('oss', '云存储（OSS）'),
                ],
                default='default',
                help_text='备份文件的存储位置',
                max_length=20,
                verbose_name='存储位置',
            ),
        ),
        migrations.AddField(
            model_name='backupstrategy',
            name='remote_protocol',
            field=models.CharField(
                blank=True,
                choices=[('ssh', 'SSH'), ('ftp', 'FTP'), ('http', 'HTTP')],
                help_text='远程服务器传输协议',
                max_length=10,
                verbose_name='远程协议',
            ),
        ),
        migrations.AddField(
            model_name='backupstrategy',
            name='remote_host',
            field=models.CharField(
                blank=True,
                help_text='远程服务器地址',
                max_length=255,
                verbose_name='远程主机',
            ),
        ),
        migrations.AddField(
            model_name='backupstrategy',
            name='remote_port',
            field=models.PositiveIntegerField(
                blank=True,
                help_text='远程服务器端口',
                null=True,
                verbose_name='远程端口',
            ),
        ),
        migrations.AddField(
            model_name='backupstrategy',
            name='remote_user',
            field=models.CharField(
                blank=True,
                help_text='远程服务器用户名',
                max_length=100,
                verbose_name='远程用户名',
            ),
        ),
        migrations.AddField(
            model_name='backupstrategy',
            name='remote_password',
            field=models.TextField(
                blank=True,
                help_text='加密存储的远程服务器密码',
                verbose_name='远程密码',
            ),
        ),
        migrations.AddField(
            model_name='backupstrategy',
            name='remote_key_path',
            field=models.CharField(
                blank=True,
                help_text='远程服务器私钥路径（优先于密码）',
                max_length=500,
                verbose_name='远程密钥路径',
            ),
        ),
        migrations.AddField(
            model_name='backupstrategy',
            name='oss_endpoint',
            field=models.CharField(
                blank=True,
                help_text='对象存储 Endpoint',
                max_length=255,
                verbose_name='OSS Endpoint',
            ),
        ),
        migrations.AddField(
            model_name='backupstrategy',
            name='oss_access_key_id',
            field=models.CharField(
                blank=True,
                help_text='对象存储 AccessKey ID',
                max_length=255,
                verbose_name='OSS AccessKey',
            ),
        ),
        migrations.AddField(
            model_name='backupstrategy',
            name='oss_access_key_secret',
            field=models.TextField(
                blank=True,
                help_text='加密存储的对象存储密钥',
                verbose_name='OSS AccessKey Secret',
            ),
        ),
        migrations.AddField(
            model_name='backupstrategy',
            name='oss_bucket',
            field=models.CharField(
                blank=True,
                help_text='对象存储 Bucket 名称',
                max_length=255,
                verbose_name='OSS Bucket',
            ),
        ),
        migrations.AddField(
            model_name='backupstrategy',
            name='oss_prefix',
            field=models.CharField(
                blank=True,
                help_text='对象存储路径前缀',
                max_length=255,
                verbose_name='OSS 路径',
            ),
        ),
        migrations.AddField(
            model_name='backuponeofftask',
            name='storage_mode',
            field=models.CharField(
                choices=[
                    ('default', '默认容器路径'),
                    ('mysql_host', 'MySQL 服务器路径'),
                    ('remote_server', '远程服务器路径'),
                    ('oss', '云存储（OSS）'),
                ],
                default='default',
                help_text='备份文件的存储位置',
                max_length=20,
                verbose_name='存储位置',
            ),
        ),
        migrations.AddField(
            model_name='backuponeofftask',
            name='remote_protocol',
            field=models.CharField(
                blank=True,
                choices=[('ssh', 'SSH'), ('ftp', 'FTP'), ('http', 'HTTP')],
                help_text='远程服务器传输协议',
                max_length=10,
                verbose_name='远程协议',
            ),
        ),
        migrations.AddField(
            model_name='backuponeofftask',
            name='remote_host',
            field=models.CharField(
                blank=True,
                help_text='远程服务器地址',
                max_length=255,
                verbose_name='远程主机',
            ),
        ),
        migrations.AddField(
            model_name='backuponeofftask',
            name='remote_port',
            field=models.PositiveIntegerField(
                blank=True,
                help_text='远程服务器端口',
                null=True,
                verbose_name='远程端口',
            ),
        ),
        migrations.AddField(
            model_name='backuponeofftask',
            name='remote_user',
            field=models.CharField(
                blank=True,
                help_text='远程服务器用户名',
                max_length=100,
                verbose_name='远程用户名',
            ),
        ),
        migrations.AddField(
            model_name='backuponeofftask',
            name='remote_password',
            field=models.TextField(
                blank=True,
                help_text='加密存储的远程服务器密码',
                verbose_name='远程密码',
            ),
        ),
        migrations.AddField(
            model_name='backuponeofftask',
            name='remote_key_path',
            field=models.CharField(
                blank=True,
                help_text='远程服务器私钥路径（优先于密码）',
                max_length=500,
                verbose_name='远程密钥路径',
            ),
        ),
        migrations.AddField(
            model_name='backuponeofftask',
            name='oss_endpoint',
            field=models.CharField(
                blank=True,
                help_text='对象存储 Endpoint',
                max_length=255,
                verbose_name='OSS Endpoint',
            ),
        ),
        migrations.AddField(
            model_name='backuponeofftask',
            name='oss_access_key_id',
            field=models.CharField(
                blank=True,
                help_text='对象存储 AccessKey ID',
                max_length=255,
                verbose_name='OSS AccessKey',
            ),
        ),
        migrations.AddField(
            model_name='backuponeofftask',
            name='oss_access_key_secret',
            field=models.TextField(
                blank=True,
                help_text='加密存储的对象存储密钥',
                verbose_name='OSS AccessKey Secret',
            ),
        ),
        migrations.AddField(
            model_name='backuponeofftask',
            name='oss_bucket',
            field=models.CharField(
                blank=True,
                help_text='对象存储 Bucket 名称',
                max_length=255,
                verbose_name='OSS Bucket',
            ),
        ),
        migrations.AddField(
            model_name='backuponeofftask',
            name='oss_prefix',
            field=models.CharField(
                blank=True,
                help_text='对象存储路径前缀',
                max_length=255,
                verbose_name='OSS 路径',
            ),
        ),
        migrations.AddField(
            model_name='backuprecord',
            name='remote_protocol',
            field=models.CharField(
                blank=True,
                choices=[('ssh', 'SSH'), ('ftp', 'FTP'), ('http', 'HTTP')],
                help_text='远程服务器传输协议',
                max_length=10,
                verbose_name='远程协议',
            ),
        ),
        migrations.AddField(
            model_name='backuprecord',
            name='remote_host',
            field=models.CharField(
                blank=True,
                help_text='远程服务器地址',
                max_length=255,
                verbose_name='远程主机',
            ),
        ),
        migrations.AddField(
            model_name='backuprecord',
            name='remote_port',
            field=models.PositiveIntegerField(
                blank=True,
                help_text='远程服务器端口',
                null=True,
                verbose_name='远程端口',
            ),
        ),
        migrations.AddField(
            model_name='backuprecord',
            name='remote_user',
            field=models.CharField(
                blank=True,
                help_text='远程服务器用户名',
                max_length=100,
                verbose_name='远程用户名',
            ),
        ),
        migrations.AddField(
            model_name='backuprecord',
            name='remote_password',
            field=models.TextField(
                blank=True,
                help_text='加密存储的远程服务器密码',
                verbose_name='远程密码',
            ),
        ),
        migrations.AddField(
            model_name='backuprecord',
            name='remote_key_path',
            field=models.CharField(
                blank=True,
                help_text='远程服务器私钥路径（优先于密码）',
                max_length=500,
                verbose_name='远程密钥路径',
            ),
        ),
    ]
