from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("dms", "0004_priority_and_manual_mode"),
    ]

    operations = [
        migrations.AddField(
            model_name="document",
            name="external_job_id",
            field=models.PositiveIntegerField(
                blank=True,
                db_index=True,
                help_text="External pl_job_master.JobID when imported from SFTP/MySQL.",
                null=True,
            ),
        ),
        migrations.AddField(
            model_name="document",
            name="external_job_name",
            field=models.CharField(
                blank=True,
                default="",
                help_text="External pl_job_master.JobName when imported from SFTP/MySQL.",
                max_length=128,
            ),
        ),
        migrations.AddField(
            model_name="document",
            name="external_job_user_file_id",
            field=models.PositiveIntegerField(
                blank=True,
                db_index=True,
                help_text="External pl_job_file_user.JobUserFileID for idempotent import.",
                null=True,
            ),
        ),
    ]
