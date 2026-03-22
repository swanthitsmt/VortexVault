param(
  [string]$PythonExe = "python"
)

$ErrorActionPreference = "Stop"

if (-not (Test-Path ".env")) {
  Copy-Item ".env.example" ".env"
}

& $PythonExe -m venv .venv
& .\.venv\Scripts\python.exe -m pip install --upgrade pip
& .\.venv\Scripts\pip.exe install -r requirements.txt

Write-Host "1) Start PostgreSQL 15+ and Redis locally"
Write-Host "2) Update DATABASE_URL/REDIS in .env"
Write-Host "3) Start API: .\.venv\Scripts\uvicorn app.main:app --host 0.0.0.0 --port 8000"
Write-Host "4) Start worker: .\.venv\Scripts\celery -A app.celery_app.celery_app worker --loglevel=INFO"
