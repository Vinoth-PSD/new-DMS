import os
import posixpath
import re
from dataclasses import dataclass
from datetime import datetime

import paramiko
import pymysql


_VERSION_RE = re.compile(r"^v(\d+)$", re.IGNORECASE)


@dataclass
class ExternalFilePick:
    job_name: str
    db_path: str
    input_base_dir: str
    selected_source_dir: str
    selected_source_file: str
    selected_version: str
    source_payload: bytes
    cleanup_dir: str


def _env(name: str, default: str = "") -> str:
    return (os.getenv(name, default) or "").strip()


def _join(*parts: str) -> str:
    clean = [p.strip("/") for p in parts if p and p.strip("/")]
    if not clean:
        return "/"
    return "/" + "/".join(clean)


def _list_dir(sftp: paramiko.SFTPClient, directory: str) -> list[paramiko.SFTPAttributes]:
    try:
        return sftp.listdir_attr(directory)
    except FileNotFoundError:
        return []
    except OSError:
        return []


def _is_dir_mode(mode: int) -> bool:
    return bool(mode & 0o040000)


def _is_file_mode(mode: int) -> bool:
    return bool(mode & 0o100000)


def _parse_input_base(path_hint: str, job_name: str) -> str:
    p = "/" + (path_hint or "").strip("/")
    lower = p.lower()
    for token in ("/1 input", "/input"):
        idx = lower.find(token)
        if idx != -1:
            return p[: idx + len(token)]
    # fallback if hint is already job root only
    return _join(job_name, "input")


def _derive_cleanup_dir(input_base_dir: str) -> str:
    normalized = "/" + input_base_dir.strip("/")
    for token in ("/1 input", "/input"):
        idx = normalized.lower().rfind(token)
        if idx != -1:
            root = normalized[:idx]
            return _join(root, "2 Cleanup")
    return _join(normalized, "2 Cleanup")


def _pick_latest_file_in_dir(
    sftp: paramiko.SFTPClient, directory: str
) -> tuple[str, bytes] | tuple[None, None]:
    rows = [x for x in _list_dir(sftp, directory) if _is_file_mode(x.st_mode)]
    if not rows:
        return None, None
    rows.sort(key=lambda x: int(getattr(x, "st_mtime", 0) or 0), reverse=True)
    chosen = rows[0]
    remote_file = _join(directory, chosen.filename)
    with sftp.open(remote_file, "rb") as stream:
        payload = stream.read()
    return remote_file, payload


def _pick_source_file(
    sftp: paramiko.SFTPClient, input_base_dir: str
) -> tuple[str, str, bytes]:
    children = [x for x in _list_dir(sftp, input_base_dir) if _is_dir_mode(x.st_mode)]
    versions: list[tuple[int, str]] = []
    for item in children:
        m = _VERSION_RE.match(item.filename or "")
        if not m:
            continue
        versions.append((int(m.group(1)), item.filename))
    versions.sort(key=lambda x: x[0], reverse=True)
    # Prefer latest version (V10 > V2), but fallback to older versions if latest is empty.
    for n, label in versions:
        source_dir = _join(input_base_dir, label)
        remote_file, payload = _pick_latest_file_in_dir(sftp, source_dir)
        if remote_file and payload:
            return source_dir, f"V{n}", payload
    # No version folders (or empty): use direct input folder.
    remote_file, payload = _pick_latest_file_in_dir(sftp, input_base_dir)
    if remote_file and payload:
        return input_base_dir, "DIRECT", payload
    raise FileNotFoundError(f"No input files found in {input_base_dir}")


def _mysql_connection():
    return pymysql.connect(
        host=_env("EXTERNAL_MYSQL_HOST"),
        port=int(_env("EXTERNAL_MYSQL_PORT", "3306")),
        user=_env("EXTERNAL_MYSQL_USER"),
        password=_env("EXTERNAL_MYSQL_PASSWORD"),
        database=_env("EXTERNAL_MYSQL_DB"),
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
    )


def _sftp_connection():
    transport = paramiko.Transport((_env("SFTP_HOST"), int(_env("SFTP_PORT", "22"))))
    transport.connect(username=_env("SFTP_USERNAME"), password=_env("SFTP_PASSWORD"))
    client = paramiko.SFTPClient.from_transport(transport)
    return transport, client


def _get_db_row_for_job(job_name: str) -> dict:
    query = """
        SELECT JobUserFileID, JobName, JobUserFileName, JobUserFilePath
        FROM pl_job_file_user
        WHERE JobName = %s
        ORDER BY JobUserFileID DESC
        LIMIT 1
    """
    with _mysql_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (job_name,))
            row = cur.fetchone()
    if not row:
        raise ValueError(f"No pl_job_file_user rows found for job '{job_name}'")
    return row


def fetch_latest_job_input(job_name: str) -> ExternalFilePick:
    row = _get_db_row_for_job(job_name)
    path_hint = str(row.get("JobUserFilePath") or "")
    input_base_dir = _parse_input_base(path_hint, job_name)
    transport, sftp = _sftp_connection()
    try:
        source_dir, selected_version, payload = _pick_source_file(sftp, input_base_dir)
        remote_file, _ = _pick_latest_file_in_dir(sftp, source_dir)
        cleanup_dir = _derive_cleanup_dir(input_base_dir)
        return ExternalFilePick(
            job_name=job_name,
            db_path=path_hint,
            input_base_dir=input_base_dir,
            selected_source_dir=source_dir,
            selected_source_file=remote_file or "",
            selected_version=selected_version,
            source_payload=payload,
            cleanup_dir=cleanup_dir,
        )
    finally:
        try:
            sftp.close()
        finally:
            transport.close()


def upload_to_cleanup_dir(cleanup_dir: str, filename: str, payload: bytes) -> str:
    transport, sftp = _sftp_connection()
    try:
        # Ensure cleanup path exists recursively.
        current = ""
        for chunk in cleanup_dir.strip("/").split("/"):
            current = _join(current, chunk)
            try:
                sftp.listdir(current)
            except Exception:
                sftp.mkdir(current)
        out_remote = _join(cleanup_dir, filename)
        with sftp.open(out_remote, "wb") as stream:
            stream.write(payload)
        return out_remote
    finally:
        try:
            sftp.close()
        finally:
            transport.close()


def build_cleanup_filename(job_name: str, ext: str) -> str:
    ts = datetime.utcnow().strftime("%Y%m%d%H%M%S")
    safe = re.sub(r"[^A-Za-z0-9_-]+", "_", job_name).strip("_") or "job"
    return f"{safe}_cleanup_{ts}{ext}"
