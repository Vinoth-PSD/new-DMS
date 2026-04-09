$ErrorActionPreference = "Stop"

if (!(Test-Path ".venv")) {
    python -m venv .venv
}
& ".\.venv\Scripts\Activate.ps1"
pip install -r requirements.txt

pyinstaller `
  --noconfirm `
  --clean `
  --name "DocProResourceTray" `
  --onefile `
  --noconsole `
  tray_app.py

Copy-Item ".\config.example.json" ".\dist\config.example.json" -Force
Write-Host "Built: dist\DocProResourceTray.exe"
Write-Host "Copy config.example.json to dist\config.json and edit paths + base_url."
