"""Archive superseded merged outputs and bump revision counters."""

from __future__ import annotations

import re
from pathlib import Path

from django.core.files.base import ContentFile
from django.utils import timezone
from django.utils.text import get_valid_filename

from .models import Document, MergedFileVersion


def archive_current_merged_snapshot(document: Document, actor=None) -> None:
    """Move current final_merged_file into MergedFileVersion before replacing it."""
    rev = document.merged_revision or 0
    if rev < 1 or not document.final_merged_file or not document.final_merged_file.name:
        return
    with document.final_merged_file.open("rb") as f:
        data = f.read()
    ext = Path(document.final_merged_file.name).suffix or ".docx"
    MergedFileVersion.objects.create(
        document=document,
        version=rev,
        file=ContentFile(data, name=f"merged_v{rev}_{document.id}{ext}"),
        created_by=actor,
    )


def finalize_merged_output(
    document: Document,
    payload: bytes,
    storage_name: str,
    *,
    actor=None,
) -> None:
    """Replace merged file with new bytes, archiving the previous revision when applicable."""
    archive_current_merged_snapshot(document, actor=actor)
    document.final_merged_file.save(storage_name, ContentFile(payload), save=False)
    document.merged_revision = (document.merged_revision or 0) + 1
    document.merged_at = timezone.now()
    document.status = Document.Status.COMPLETED
    document.save(
        update_fields=["final_merged_file", "merged_revision", "status", "merged_at", "updated_at"]
    )


def suggested_merged_download_filename(document: Document) -> str:
    """Browser download name: {job_id}_{title}_{datetime}_merged.ext"""
    stem = re.sub(r"[^A-Za-z0-9_-]+", "_", document.title or "document").strip("_")[:100] or "document"
    ts = timezone.now().strftime("%Y%m%d_%H%M%S")
    base = document.final_merged_file.name if document.final_merged_file else ""
    ext = Path(base).suffix.lower() if base else ".docx"
    if ext not in (".docx", ".doc", ".pdf", ".zip"):
        ext = ".docx"
    raw = f"{document.id}_{stem}_{ts}_merged{ext}"
    return get_valid_filename(raw)
