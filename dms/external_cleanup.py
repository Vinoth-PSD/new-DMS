import os
import posixpath
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import paramiko
import pymysql


_VERSION_RE = re.compile(r"^v(\d+)$", re.IGNORECASE)
_ENV_LOADED = False


def _ensure_env_loaded() -> None:
    """Load project .env lazily so long-lived web processes pick latest values."""
    global _ENV_LOADED
    if _ENV_LOADED:
        return
    try:
        from dotenv import load_dotenv

        root = Path(__file__).resolve().parents[1]
        load_dotenv(root / ".env", override=True)
    except Exception:
        pass
    _ENV_LOADED = True


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
    _ensure_env_loaded()
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
    cleanup_name = (_env("EXTERNAL_CLEANUP_DIR_NAME") or "2 Cleanup").strip("/") or "2 Cleanup"
    for token in ("/1 input", "/input"):
        idx = normalized.lower().rfind(token)
        if idx != -1:
            root = normalized[:idx]
            return _join(root, cleanup_name)
    return _join(normalized, cleanup_name)


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


def _sftp_host() -> str:
    return _env("SFTP_HOST")


def _sftp_port() -> int:
    return int(_env("SFTP_PORT", "22") or "22")


def _sftp_username() -> str:
    return _env("SFTP_USERNAME") or _env("SFTP_USER")


def _sftp_password() -> str:
    return _env("SFTP_PASSWORD") or _env("SFTP_PASS")


def _sftp_connection():
    transport = paramiko.Transport((_sftp_host(), _sftp_port()))
    transport.connect(username=_sftp_username(), password=_sftp_password())
    client = paramiko.SFTPClient.from_transport(transport)
    return transport, client


def sftp_read_remote_file(remote_path: str) -> bytes:
    """Read an entire file from SFTP using an absolute remote path (e.g. JobUserFilePath)."""
    path = "/" + (remote_path or "").strip().strip("/")
    transport, sftp = _sftp_connection()
    try:
        with sftp.open(path, "rb") as stream:
            return stream.read()
    finally:
        try:
            sftp.close()
        finally:
            transport.close()


def ensure_remote_dir(sftp: paramiko.SFTPClient, directory: str) -> str:
    current = ""
    for chunk in (directory or "").strip("/").split("/"):
        if not chunk:
            continue
        current = _join(current, chunk)
        try:
            sftp.listdir(current)
        except Exception:
            sftp.mkdir(current)
    return "/" + (directory or "").strip("/")


def create_job_folder_structure(job_name: str, client_root_dir: str, versions: int = 1) -> dict:
    """
    Create SFTP job folders:
      /<client_root>/<job_name>/1 Input
      /<client_root>/<job_name>/1 Input/V1..Vn
      /<client_root>/<job_name>/2 Cleanup
    """
    safe_job = re.sub(r"[\\/]+", "_", (job_name or "").strip()).strip("_")
    if not safe_job:
        raise ValueError("job_name is required")
    if versions < 1:
        raise ValueError("versions must be >= 1")

    job_root = _join(client_root_dir, safe_job)
    input_dir = _join(job_root, "1 Input")
    cleanup_dir = _join(job_root, "2 Cleanup")
    version_dirs = [_join(input_dir, f"V{i}") for i in range(1, versions + 1)]

    transport, sftp = _sftp_connection()
    try:
        ensure_remote_dir(sftp, input_dir)
        for vdir in version_dirs:
            ensure_remote_dir(sftp, vdir)
        ensure_remote_dir(sftp, cleanup_dir)
        return {
            "job_root": job_root,
            "input_dir": input_dir,
            "cleanup_dir": cleanup_dir,
            "version_dirs": version_dirs,
        }
    finally:
        try:
            sftp.close()
        finally:
            transport.close()


def _job_ready_status_id() -> int:
    return int(_env("EXTERNAL_JOB_READY_STATUS_ID", "1008"))


def _get_db_row_for_job(job_key: str) -> dict:
    """
    Resolve the latest input file for a job when pl_job_master indicates it is ready.

    Rules:
    - Join pl_job_file_user (f) to pl_job_master (j) on JobID.
    - Require j.JobStatusID == EXTERNAL_JOB_READY_STATUS_ID (default 1008).
    - If multiple file rows exist for the same JobID, pick the one with highest JobUserFileID.

    job_key may be pl_job_master.JobName (e.g. XBSG1) or numeric pl_job_master.JobID as string.
    """
    job_key = (job_key or "").strip()
    if not job_key:
        raise ValueError("job_name or job_id is required")

    status_id = _job_ready_status_id()
    base_sql = """
        SELECT
            f.JobUserFileID,
            f.JobID,
            f.JobName,
            f.JobUserFileName,
            f.JobUserFilePath,
            j.JobName AS master_job_name,
            j.JobStatusID
        FROM pl_job_file_user f
        INNER JOIN pl_job_master j ON j.JobID = f.JobID
        WHERE j.JobStatusID = %s
          AND {job_match}
        ORDER BY f.JobUserFileID DESC
        LIMIT 1
    """
    with _mysql_connection() as conn:
        with conn.cursor() as cur:
            if job_key.isdigit():
                sql = base_sql.format(job_match="j.JobID = %s")
                cur.execute(sql, (status_id, int(job_key)))
            else:
                sql = base_sql.format(job_match="j.JobName = %s")
                cur.execute(sql, (status_id, job_key))
            row = cur.fetchone()
    if not row:
        raise ValueError(
            f"No pl_job_file_user row for job key '{job_key}' with pl_job_master.JobStatusID = {status_id}. "
            "Confirm the job exists, status is correct, and pl_job_file_user has at least one row for that JobID."
        )
    return row


def fetch_latest_job_input(job_name: str) -> ExternalFilePick:
    row = _get_db_row_for_job(job_name)
    path_hint = str(row.get("JobUserFilePath") or "")
    resolved_name = str(row.get("master_job_name") or row.get("JobName") or job_name).strip() or job_name
    input_base_dir = _parse_input_base(path_hint, resolved_name)
    transport, sftp = _sftp_connection()
    try:
        source_dir, selected_version, payload = _pick_source_file(sftp, input_base_dir)
        remote_file, _ = _pick_latest_file_in_dir(sftp, source_dir)
        cleanup_dir = _derive_cleanup_dir(input_base_dir)
        return ExternalFilePick(
            job_name=resolved_name,
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
        ensure_remote_dir(sftp, cleanup_dir)
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
