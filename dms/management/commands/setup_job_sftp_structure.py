from django.core.management.base import BaseCommand, CommandError

from dms.external_cleanup import create_job_folder_structure


class Command(BaseCommand):
    help = "Create SFTP folder structure for a job (1 Input/V* and 2 Cleanup)."

    def add_arguments(self, parser):
        parser.add_argument("--job-name", required=True, help="Job name (e.g. XBSG1)")
        parser.add_argument(
            "--client-root",
            required=True,
            help="Client root folder on SFTP (e.g. /sample_clientfiles/xberra_sg)",
        )
        parser.add_argument(
            "--versions",
            type=int,
            default=1,
            help="Number of input version folders to create under 1 Input (default: 1)",
        )

    def handle(self, *args, **options):
        job_name = options["job_name"]
        client_root = options["client_root"]
        versions = int(options["versions"])

        try:
            result = create_job_folder_structure(
                job_name=job_name,
                client_root_dir=client_root,
                versions=versions,
            )
        except Exception as exc:
            raise CommandError(str(exc)) from exc

        self.stdout.write(self.style.SUCCESS("SFTP structure created successfully."))
        self.stdout.write(f"job_root: {result['job_root']}")
        self.stdout.write(f"input_dir: {result['input_dir']}")
        self.stdout.write(f"cleanup_dir: {result['cleanup_dir']}")
        self.stdout.write("version_dirs:")
        for vdir in result["version_dirs"]:
            self.stdout.write(f"  - {vdir}")
