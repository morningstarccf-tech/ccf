from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('sqlclient', '0001_initial'),
    ]

    operations = [
        migrations.CreateModel(
            name='SQLTerminal',
            fields=[],
            options={
                'proxy': True,
                'verbose_name': 'SQL终端',
                'verbose_name_plural': 'SQL终端',
            },
            bases=('sqlclient.queryhistory',),
        ),
    ]
