"""
Verify external MySQL and SFTP settings (no secrets printed).

Uses the same env vars as dms/external_cleanup.py:
  EXTERNAL_MYSQL_*  SFTP_*
"""

from django.core.management.base import BaseCommand

import paramiko
import pymysql
from django.conf import settings
from django.db import connection


class Command(BaseCommand):
    help = "Test EXTERNAL_MYSQL_* and SFTP_* connectivity (passwords are not shown)."

    def handle(self, *args, **options):
        self._check_django_db()
        self._check_mysql()
        self._check_sftp()

    def _check_django_db(self) -> None:
        """Default Django database (e.g. PostgreSQL for the DMS app)."""
        alias = "default"
        try:
            conn = connection
            conn.ensure_connection()
            with conn.cursor() as cur:
                if conn.vendor == "postgresql":
                    cur.execute("SELECT 1")
                else:
                    cur.execute("SELECT 1")
                cur.fetchone()
            db = settings.DATABASES.get(alias, {})
            host = db.get("HOST", "")
            name = db.get("NAME", "")
            user = db.get("USER", "")
            self.stdout.write(
                self.style.SUCCESS(
                    f"Django DB ({alias}): OK — engine={db.get('ENGINE','')} host={host!r} name={name!r} user={user!r}"
                )
            )
        except Exception as exc:
            self.stdout.write(self.style.ERROR(f"Django DB (default): FAILED — {exc}"))

    def _check_mysql(self) -> None:
        import os

        host = (os.getenv("EXTERNAL_MYSQL_HOST") or "").strip()
        port = int(os.getenv("EXTERNAL_MYSQL_PORT", "3306") or "3306")
        user = (os.getenv("EXTERNAL_MYSQL_USER") or "").strip()
        password = os.getenv("EXTERNAL_MYSQL_PASSWORD", "") or ""
        database = (os.getenv("EXTERNAL_MYSQL_DB") or "").strip()

        if not host or not user or not database:
            self.stdout.write(
                self.style.ERROR(
                    "MySQL: MISSING config — set EXTERNAL_MYSQL_HOST, EXTERNAL_MYSQL_USER, EXTERNAL_MYSQL_DB "
                    "(and password if required)."
                )
            )
            return

        try:
            conn = pymysql.connect(
                host=host,
                port=port,
                user=user,
                password=password,
                database=database,
                charset="utf8mb4",
                connect_timeout=10,
            )
            try:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
                    cur.fetchone()
            finally:
                conn.close()
            self.stdout.write(
                self.style.SUCCESS(f"MySQL: OK — host={host!r} port={port} user={user!r} database={database!r}")
            )
        except Exception as exc:
            self.stdout.write(
                self.style.ERROR(f"MySQL: FAILED — host={host!r} port={port} user={user!r} database={database!r} — {exc}")
            )

    def _check_sftp(self) -> None:
        import os

        host = (os.getenv("SFTP_HOST") or "").strip()
        port = int(os.getenv("SFTP_PORT", "22") or "22")
        user = (os.getenv("SFTP_USERNAME") or os.getenv("SFTP_USER") or "").strip()
        password = (os.getenv("SFTP_PASSWORD") or os.getenv("SFTP_PASS") or "") or ""

        if not host or not user:
            self.stdout.write(
                self.style.ERROR(
                    "SFTP: MISSING config — set SFTP_HOST and SFTP_USERNAME (or SFTP_USER), "
                    "and SFTP_PASSWORD (or SFTP_PASS) if required."
                )
            )
            return

        transport = None
        try:
            transport = paramiko.Transport((host, port))
            transport.connect(username=user, password=password)
            client = paramiko.SFTPClient.from_transport(transport)
            try:
                client.normalize(".")
                _ = client.getcwd()
            finally:
                client.close()
            self.stdout.write(self.style.SUCCESS(f"SFTP: OK — host={host!r} port={port} user={user!r}"))
        except Exception as exc:
            self.stdout.write(self.style.ERROR(f"SFTP: FAILED — host={host!r} port={port} user={user!r} — {exc}"))
        finally:
            if transport is not None:
                try:
                    transport.close()
                except Exception:
                    pass
