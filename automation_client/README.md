# DocPro Resource Tray (single EXE for all resources)

This EXE now runs in **process-triggered mode**:

- It does **not** auto-download jobs.
- It opens ABBYY only for **newly saved files** in `download_folder` that match:
  `resourceid_jobid_filename_bundleid.ext`
- It uploads cleaned files from `upload_folder` and reads both:
  - `resource_id` from filename prefix
  - `job_id` from filename segment

No username/password and no manual per-user EXE setup.

## Required naming

- Downloaded/processed file format:
  - `resourceid_jobid_filename_bundleid.pdf`
  - `resourceid_jobid_filename_bundleid.docx`
  - `resourceid_jobid_filename_bundleid.doc`

The app also accepts:
- `resourceid_<title>_B<jobid>.ext`
- `resourceid_<title>_<jobid>.ext`

## Important behavior

1. ABBYY opens only for **new files created after tray starts**.
2. Existing files already in `download_folder` are ignored.
3. The same downloaded file is opened once (tracked in memory per run).

## Build

```powershell
cd automation_client
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
.\build.ps1
```

Output:
- `dist\DocProResourceTray.exe`
- `dist\config.example.json`

## Deploy

1. Copy `DocProResourceTray.exe` and `config.json` (from `config.example.json`) to resource PC.
2. Set browser download path to same folder as `download_folder`.
3. Start tray app (icon in system tray).
4. Click **Process** in web UI; saved file opens in ABBYY.
5. Save cleaned file into `upload_folder` with same naming pattern; tray auto-uploads.
