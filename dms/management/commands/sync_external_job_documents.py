from django.core.management.base import BaseCommand, CommandError

from dms.external_import import sync_external_job_documents


class Command(BaseCommand):
    help = "Import eligible job input files from external MySQL + SFTP into local Documents."

    def handle(self, *args, **options):
        try:
            result = sync_external_job_documents(uploaded_by=None)
        except Exception as exc:
            raise CommandError(str(exc)) from exc

        self.stdout.write(self.style.SUCCESS(f"Examined: {result['examined']}, created: {result['created_count']}"))
        if result["created_document_ids"]:
            self.stdout.write("New document IDs: " + ", ".join(str(i) for i in result["created_document_ids"]))
        if result["skipped"]:
            self.stdout.write(f"Skipped: {len(result['skipped'])}")
        if result["errors"]:
            self.stdout.write(self.style.WARNING(f"Errors: {len(result['errors'])}"))
            for err in result["errors"][:20]:
                self.stdout.write(str(err))
