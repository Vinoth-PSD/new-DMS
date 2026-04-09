"""
DocPro resource tray application (one Windows .exe for all resources).

Requirements covered:
- Background + system tray (no console when built with PyInstaller --noconsole).
- No username/password: only Resource ID + Job ID (document id), carried in filenames
  and sent as resource_id / job_id to the existing Django automation API.
- Naming: resourceid_jobid_filename_bundleid.ext for downloads (server) and cleaned uploads.
- Opens bundles in ABBYY FineReader when possible, otherwise default handler.

API (unchanged on server):
  GET  /api/automation/jobs/?resource_id=<id>
  GET  /api/automation/jobs/<job_id>/download/?resource_id=<id>
  POST /api/automation/jobs/<job_id>/submit/  (multipart processed_file + resource_id)
"""

from __future__ import annotations

import json
import logging
import os
import re
import subprocess
import sys
import threading
import time
from pathlib import Path

import pystray
import requests
from PIL import Image, ImageDraw
from pystray import MenuItem as item

# Strict name: resourceid_jobid_title_bundleid.ext
STRICT_UPLOAD_NAME = re.compile(
    r"^(?P<resourceid>\d+)_(?P<jobid>\d+)_(?P<filename>.+?)_(?P<bundleid>[^_.]+)\.(?P<ext>pdf|docx|doc)$",
    re.IGNORECASE,
)

INFER_FROM_DOWNLOAD = re.compile(r"^(\d+)_(\d+)_")
MAX_UPLOAD_FAILURES = 5


def app_dir() -> Path:
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent
    return Path(__file__).resolve().parent


def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[logging.FileHandler(app_dir() / "automation.log", encoding="utf-8")],
    )


def _fatal_msg(msg: str) -> None:
    logging.error(msg)
    if getattr(sys, "frozen", False) and sys.platform == "win32":
        try:
            import ctypes

            ctypes.windll.user32.MessageBoxW(0, msg, "DocPro Resource Tray", 0x10)
        except Exception:
            pass


def load_json_config() -> dict:
    base = app_dir()
    path = base / "config.json"
    sample = base / "config.example.json"
    if not path.exists():
        raise RuntimeError(
            f"Missing config.json in {base}. Copy config.example.json to config.json and set base_url and folders."
        )
    return json.loads(path.read_text(encoding="utf-8"))


def parse_runtime_config(raw: dict) -> dict:
    if not isinstance(raw, dict):
        raise RuntimeError("config.json must be a JSON object.")

    base_url = (raw.get("base_url") or raw.get("baseUrl") or "").strip().rstrip("/")
    if not base_url:
        raise RuntimeError('config.json needs "base_url" (e.g. https://your-host).')

    dl = (raw.get("download_folder") or raw.get("downloadFolder") or "").strip()
    ul = (raw.get("upload_folder") or raw.get("uploadFolder") or "").strip()
    if not dl or not ul:
        raise RuntimeError('config.json needs "download_folder" and "upload_folder".')

    return {
        "base_url": base_url,
        "download_folder": Path(dl),
        "upload_folder": Path(ul),
        "poll_seconds": int(raw.get("poll_seconds", 5)),
        "watch_seconds": int(raw.get("watch_seconds", 2)),
        "abbyy_exe_path": (raw.get("abbyy_exe_path") or raw.get("abbyyExePath") or "").strip(),
        # EXE behavior is process-triggered: open only newly downloaded bundles.
        "process_triggered_open_only": bool(raw.get("process_triggered_open_only", True)),
    }


class ResourceTrayApp:
    def __init__(self, cfg: dict):
        self.base_url = cfg["base_url"]
        self.download_dir: Path = cfg["download_folder"]
        self.upload_dir: Path = cfg["upload_folder"]
        self.poll_seconds = cfg["poll_seconds"]
        self.watch_seconds = cfg["watch_seconds"]
        self.abbyy_hint = cfg["abbyy_exe_path"]
        self.process_triggered_open_only = cfg["process_triggered_open_only"]

        self.session = requests.Session()
        self.running = True
        self.uploaded_paths: set[str] = set()
        self.upload_failures: dict[str, int] = {}
        self.seen_download_paths: set[str] = set()

        self.download_dir.mkdir(parents=True, exist_ok=True)
        self.upload_dir.mkdir(parents=True, exist_ok=True)
        for existing in self.download_dir.iterdir():
            if existing.is_file():
                self.seen_download_paths.add(str(existing.resolve()))

        logging.info(
            "[START] base_url=%s download=%s upload=%s mode=process-triggered",
            self.base_url,
            self.download_dir,
            self.upload_dir,
        )

    def detect_abbyy(self) -> Path | None:
        if self.abbyy_hint and Path(self.abbyy_hint).is_file():
            return Path(self.abbyy_hint)
        for cand in (
            r"C:\Program Files\ABBYY FineReader 15\FineReader.exe",
            r"C:\Program Files (x86)\ABBYY FineReader 14\FineReaderOCR.exe",
            r"C:\Program Files\ABBYY FineReader PDF 16\FineReaderPDF.exe",
        ):
            p = Path(cand)
            if p.is_file():
                return p
        return None

    def open_in_editor(self, path: Path) -> None:
        exe = self.detect_abbyy()
        if exe:
            try:
                subprocess.Popen([str(exe), str(path.resolve())], shell=False)
                logging.info("[OPEN] ABBYY %s -> %s", exe.name, path.name)
                return
            except Exception as exc:
                logging.warning("[OPEN] ABBYY launch failed: %s", exc)
        try:
            if sys.platform == "win32":
                os.startfile(str(path.resolve()))  # type: ignore[attr-defined]
            else:
                subprocess.Popen(["open" if sys.platform == "darwin" else "xdg-open", str(path.resolve())])
            logging.info("[OPEN] default handler -> %s", path.name)
        except Exception as exc:
            logging.error("[OPEN] failed: %s", exc)

    def parse_upload(self, fname: str) -> dict | None:
        m = STRICT_UPLOAD_NAME.match(fname)
        if m:
            return m.groupdict()
        base, ext = os.path.splitext(fname)
        ext = ext.lower().lstrip(".")
        if ext not in ("pdf", "docx", "doc"):
            return None
        parts = base.split("_")
        if len(parts) < 3 or not parts[0].isdigit():
            return None
        rid = int(parts[0])
        last = parts[-1]
        if last.upper().startswith("B") and last[1:].isdigit():
            return {
                "resourceid": str(rid),
                "jobid": str(int(last[1:])),
                "filename": "_".join(parts[1:-1]),
                "bundleid": last,
                "ext": ext,
            }
        if last.isdigit():
            jid = int(last)
            return {
                "resourceid": str(rid),
                "jobid": str(jid),
                "filename": "_".join(parts[1:-1]),
                "bundleid": f"B{jid}",
                "ext": ext,
            }
        return None

    def upload_one(self, path: Path, parsed: dict) -> None:
        full = str(path.resolve())
        resource_id = int(parsed["resourceid"])
        job_id = int(parsed["jobid"])
        url = f"{self.base_url}/api/automation/jobs/{job_id}/submit/"
        with path.open("rb") as fh:
            resp = self.session.post(
                url,
                files={"processed_file": (path.name, fh)},
                data={"resource_id": str(resource_id)},
                timeout=180,
            )
        if resp.ok:
            logging.info("[UPLOAD] ok %s", path.name)
            self.uploaded_paths.add(full)
            self.upload_failures.pop(full, None)
            return

        logging.error("[UPLOAD] fail %s -> %s %s", path.name, resp.status_code, resp.text)
        n = self.upload_failures.get(full, 0) + 1
        self.upload_failures[full] = n
        if n >= MAX_UPLOAD_FAILURES:
            logging.error("[UPLOAD] giving up on %s", path.name)

    def scan_browser_saved_downloads(self) -> None:
        """Open only new files saved by Process; never auto-open old existing files."""
        for p in self.download_dir.iterdir():
            if not p.is_file():
                continue
            full = str(p.resolve())
            if full in self.seen_download_paths:
                continue
            if p.suffix.lower() not in (".pdf", ".zip", ".docx"):
                self.seen_download_paths.add(full)
                continue
            m = INFER_FROM_DOWNLOAD.match(p.name)
            if not m:
                self.seen_download_paths.add(full)
                continue
            time.sleep(0.6)
            if not p.is_file():
                continue
            self.open_in_editor(p)
            logging.info("[WEB-IMPORT] Opened bundle saved from browser: %s", p.name)
            self.seen_download_paths.add(full)

    def scan_uploads(self) -> None:
        for p in self.upload_dir.iterdir():
            if not p.is_file():
                continue
            full = str(p.resolve())
            if full in self.uploaded_paths:
                continue
            if self.upload_failures.get(full, 0) >= MAX_UPLOAD_FAILURES:
                continue
            parsed = self.parse_upload(p.name)
            if not parsed:
                continue
            self.upload_one(p, parsed)

    def loop(self) -> None:
        while self.running:
            try:
                if self.process_triggered_open_only:
                    self.scan_browser_saved_downloads()
                self.scan_uploads()
            except requests.RequestException as exc:
                logging.error("[HTTP] %s", exc)
            except Exception as exc:
                logging.error("[LOOP] %s", exc)
            time.sleep(min(self.poll_seconds, self.watch_seconds))


def tray_icon(app: ResourceTrayApp) -> pystray.Icon:
    img = Image.new("RGB", (64, 64), color=(26, 35, 58))
    d = ImageDraw.Draw(img)
    d.rectangle((8, 8, 56, 56), outline=(59, 130, 246), width=3)
    d.text((18, 18), "DP", fill=(147, 197, 253))

    def quit_app(_icon, _item):
        app.running = False
        _icon.stop()

    return pystray.Icon(
        "DocProResourceTray",
        img,
        "DocPro Resource Tray",
        pystray.Menu(item("Quit", quit_app)),
    )


def main() -> None:
    setup_logging()
    try:
        raw = load_json_config()
        cfg = parse_runtime_config(raw)
        app = ResourceTrayApp(cfg)
        worker = threading.Thread(target=app.loop, daemon=True)
        worker.start()
        tray_icon(app).run()
    except Exception as exc:
        _fatal_msg(str(exc))
        raise


if __name__ == "__main__":
    main()
