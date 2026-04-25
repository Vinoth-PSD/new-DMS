"""
Import job input files from external MySQL + SFTP into local Document records.

Uses the same eligibility rules as cleanup fetch:
- pl_job_master.JobStatusID == EXTERNAL_JOB_READY_STATUS_ID (default 1008)
- Latest pl_job_file_user row per JobID (highest JobUserFileID)

Stored files use the same Document.original_file upload path as manual uploads.
"""

from __future__ import annotations

from django.contrib.auth.models import User
from django.core.files.base import ContentFile
from django.db import transaction

from .external_cleanup import (
    _env,
    _job_ready_status_id,
    _mysql_connection,
    _parse_input_base,
    _pick_latest_file_in_dir,
    _pick_source_file,
    _sftp_connection,
    _join,
)
from .models import AuditLog, Document
from .services import detect_file_type, get_total_pages
from .tasks import split_document_task


_LIST_ELIGIBLE_SQL = """
SELECT
    f.JobUserFileID,
    f.JobID,
    f.JobName,
    f.JobUserFileName,
    f.JobUserFilePath,
    j.JobName AS master_job_name
FROM pl_job_file_user f
INNER JOIN pl_job_master j ON j.JobID = f.JobID AND j.JobStatusID = %s
INNER JOIN (
    SELECT f2.JobID, MAX(f2.JobUserFileID) AS max_id
    FROM pl_job_file_user f2
    INNER JOIN pl_job_master j2 ON j2.JobID = f2.JobID AND j2.JobStatusID = %s
    GROUP BY f2.JobID
) latest ON latest.JobID = f.JobID AND latest.max_id = f.JobUserFileID
ORDER BY f.JobUserFileID ASC
"""


def list_eligible_external_import_rows() -> list[dict]:
    status_id = _job_ready_status_id()
    with _mysql_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(_LIST_ELIGIBLE_SQL, (status_id, status_id))
            return list(cur.fetchall())


def _fetch_payload_for_row(path_hint: str, job_name: str) -> tuple[str, bytes]:
    """
    Resolve payload from JobUserFilePath robustly:
    - If path points to a file, read that file.
    - If path points to a directory, pick latest file in that directory.
    - If path is invalid, fallback to input-base/version scan.
    """
    normalized = _join(path_hint)
    transport, sftp = _sftp_connection()
    try:
        def _resolve_case_insensitive_path(remote_path: str) -> str:
            parts = [p for p in remote_path.strip("/").split("/") if p]
            current = "/"
            for part in parts:
                entries = _pick_dir_entries(current)
                # Prefer exact first, then case-insensitive fallback.
                exact = next((e for e in entries if e.filename == part), None)
                if exact is not None:
                    current = _join(current, exact.filename)
                    continue
                folded = next((e for e in entries if (e.filename or "").lower() == part.lower()), None)
                if folded is not None:
                    current = _join(current, folded.filename)
                    continue
                return remote_path
            return current

        def _pick_dir_entries(directory: str):
            try:
                return sftp.listdir_attr(directory)
            except Exception:
                return []

        def _path_candidates(base_path: str) -> list[str]:
            candidates: list[str] = []
            seen: set[str] = set()

            def add(path: str):
                p = _join(path)
                if p not in seen:
                    seen.add(p)
                    candidates.append(p)

            add(base_path)
            # Some SFTP accounts are already chrooted to /.../user, so "/user/..." fails.
            parts = [p for p in base_path.strip("/").split("/") if p]
            if parts and parts[0].lower() == "user":
                add("/" + "/".join(parts[1:]))
            prefix = (_env("SFTP_PATH_PREFIX") or "").strip("/")
            if prefix:
                add(f"/{prefix}/{base_path.strip('/')}")
            return candidates

        normalized_candidates = [_resolve_case_insensitive_path(p) for p in _path_candidates(normalized)]

        # Try direct file read first.
        for candidate in normalized_candidates:
            try:
                with sftp.open(candidate, "rb") as stream:
                    return candidate, stream.read()
            except Exception:
                continue

        # If it's a directory, pick latest file in that directory.
        for candidate in normalized_candidates:
            remote_file, payload = _pick_latest_file_in_dir(sftp, candidate)
            if remote_file and payload:
                return remote_file, payload

        # Fallback: derive input base and pick latest available source version.
        parsed_base = _parse_input_base(path_hint, job_name)
        for base_candidate in [_resolve_case_insensitive_path(p) for p in _path_candidates(parsed_base)]:
            try:
                source_dir, _, payload = _pick_source_file(sftp, base_candidate)
                remote_file, _ = _pick_latest_file_in_dir(sftp, source_dir)
                if remote_file and payload:
                    return remote_file, payload
            except Exception:
                continue
        raise FileNotFoundError(f"No input files found from path hint '{path_hint}'")
    finally:
        try:
            sftp.close()
        finally:
            transport.close()


def sync_external_job_documents(*, uploaded_by: User | None = None) -> dict:
    """
    For each eligible external file row, download from SFTP and create a Document if not already imported.

    Idempotency: skips when a Document already exists with the same external_job_user_file_id.
    """
    created: list[int] = []
    skipped: list[dict] = []
    errors: list[dict] = []
    rows = list_eligible_external_import_rows()

    for row in rows:
        juf_id = int(row["JobUserFileID"])
        if Document.objects.filter(external_job_user_file_id=juf_id).exists():
            skipped.append({"job_user_file_id": juf_id, "reason": "already_imported"})
            continue

        remote_path = str(row.get("JobUserFilePath") or "").strip()
        fname = str(row.get("JobUserFileName") or "").strip()
        if not remote_path or not fname:
            errors.append({"job_user_file_id": juf_id, "reason": "missing JobUserFilePath or JobUserFileName"})
            continue

        job_id = int(row["JobID"])
        master_name = str(row.get("master_job_name") or row.get("JobName") or "").strip()
        lowered = fname.lower()
        if not (lowered.endswith(".pdf") or lowered.endswith(".docx")):
            errors.append(
                {"job_user_file_id": juf_id, "reason": "Only PDF and DOCX are supported for import.", "filename": fname}
            )
            continue

        try:
            resolved_remote_file, payload = _fetch_payload_for_row(remote_path, master_name or str(row.get("JobName") or ""))
        except Exception as exc:
            errors.append({"job_user_file_id": juf_id, "reason": str(exc), "remote_path": remote_path})
            continue

        if not payload:
            errors.append({"job_user_file_id": juf_id, "reason": "empty file from SFTP", "remote_path": remote_path})
            continue

        title = (fname.rsplit(".", 1)[0] if "." in fname else fname)[:255]

        try:
            with transaction.atomic():
                doc = Document(
                    title=title,
                    file_type=detect_file_type(fname),
                    uploaded_by=uploaded_by,
                    external_job_id=job_id,
                    external_job_name=master_name[:128],
                    external_job_user_file_id=juf_id,
                )
                doc.original_file.save(fname, ContentFile(payload), save=False)
                doc.save()
                doc.total_pages = get_total_pages(doc)
                doc.save(update_fields=["total_pages", "updated_at"])
                AuditLog.objects.create(
                    action=AuditLog.Action.UPLOAD_DOC,
                    document=doc,
                    actor=uploaded_by,
                    metadata={
                        "source": "external_sftp_import",
                        "remote_path": remote_path,
                        "resolved_remote_file": resolved_remote_file,
                        "external_job_id": job_id,
                        "external_job_name": master_name,
                        "job_user_file_id": juf_id,
                    },
                )
        except Exception as exc:
            errors.append({"job_user_file_id": juf_id, "reason": str(exc), "remote_path": remote_path})
            continue

        split_document_task.delay(doc.id)
        created.append(doc.id)

    return {
        "examined": len(rows),
        "created_count": len(created),
        "created_document_ids": created,
        "skipped": skipped,
        "errors": errors,
    }
