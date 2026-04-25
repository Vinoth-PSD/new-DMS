from django.core.validators import MaxValueValidator, MinValueValidator
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("dms", "0005_document_external_job_fields"),
    ]

    operations = [
        migrations.AddField(
            model_name="resourceprofile",
            name="break_ended_at",
            field=models.DateTimeField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name="resourceprofile",
            name="break_started_at",
            field=models.DateTimeField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name="resourceprofile",
            name="is_on_break",
            field=models.BooleanField(default=False),
        ),
        migrations.AddField(
            model_name="resourceprofile",
            name="total_break_seconds",
            field=models.PositiveIntegerField(default=0),
        ),
        migrations.AddField(
            model_name="resourceprofile",
            name="word_split_layout_ratio",
            field=models.FloatField(
                default=0.79,
                help_text="Default word split layout ratio. Capped at 0.79.",
                validators=[MinValueValidator(0.0), MaxValueValidator(0.79)],
            ),
        ),
    ]
