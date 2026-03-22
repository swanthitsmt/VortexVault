# Local Setup Guide (Docker + Windows + macOS + Linux)

This guide is written for normal users who want to run the project at home.

## Prerequisites
- Git
- Internet connection
- At least 16GB RAM recommended for large file processing
- Enough free disk space (raw files + cleaned files + database can be very large)

## Clone Project
```bash
git clone <your-private-repo-url>
cd <repo-folder>
```

## Docker Setup (Recommended)
Use this if you want the easiest and most consistent setup.

### 1) Install Docker
- Windows: Docker Desktop
- macOS (Intel/Apple Silicon): Docker Desktop
- Linux: Docker Engine + Docker Compose plugin

### 2) Prepare environment
```bash
cp .env.example .env
```

Edit `.env` and set at least:
```env
BASIC_AUTH_USERNAME=admin
BASIC_AUTH_PASSWORD=change-this-password
```

### 3) Start application
- macOS/Linux:
```bash
./scripts/one_click_setup.sh
```
- Windows:
```bat
scripts\one_click_setup.bat
```

### 4) Open web app
- App: `http://localhost:8000`
- Flower: `http://localhost:5555`

### 5) Use correct file paths in forms
In Docker mode, use container path format:
- Input folder example: `/app/data/input`
- Output file example: `/app/data/output/cleaned_format.txt`

Store your files in host project directory:
- `<repo>/data/input`
- `<repo>/data/output`

Because `./data` is mounted to `/app/data`.

### 6) Stop application
```bash
docker compose down
```

## Windows Native Setup (No Docker)
Use this only if you do not want Docker.

### 1) Install required software
- Python 3.11+
- PostgreSQL 15+
- Redis-compatible server:
  - Option A: Memurai (recommended on pure Windows)
  - Option B: Redis inside WSL2

### 2) Create Python environment
Open PowerShell in project folder:
```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install --upgrade pip
pip install -r requirements.txt
```

### 3) Configure PostgreSQL
Open SQL Shell (`psql`) and run:
```sql
CREATE ROLE combo WITH LOGIN PASSWORD 'combo';
CREATE DATABASE combo_db OWNER combo;
\c combo_db
CREATE EXTENSION IF NOT EXISTS pg_trgm;
\q
```

### 4) Configure `.env`
```powershell
copy .env.example .env
```
Set these values in `.env`:
```env
DATABASE_URL=postgresql+asyncpg://combo:combo@localhost:5432/combo_db
REDIS_URL=redis://localhost:6379/0
CELERY_BROKER_URL=redis://localhost:6379/1
CELERY_RESULT_BACKEND=redis://localhost:6379/2
BASIC_AUTH_USERNAME=admin
BASIC_AUTH_PASSWORD=change-this-password
```

### 5) Start app processes
Terminal 1:
```powershell
.\.venv\Scripts\uvicorn app.main:app --host 0.0.0.0 --port 8000
```

Terminal 2:
```powershell
.\.venv\Scripts\celery -A app.celery_app.celery_app worker --loglevel=INFO
```

Optional terminal 3:
```powershell
.\.venv\Scripts\celery -A app.celery_app.celery_app flower --port=5555
```

### 6) File path format in UI
Use absolute Windows paths:
- Input folder: `C:\combo\input`
- Output file: `C:\combo\output\cleaned_format.txt`

## macOS Native Setup (No Docker)

### 1) Install dependencies
With Homebrew:
```bash
brew update
brew install python@3.12 postgresql@15 redis
brew services start postgresql@15
brew services start redis
```

### 2) Create DB/user/extension
```bash
psql postgres
```
Then run:
```sql
CREATE ROLE combo WITH LOGIN PASSWORD 'combo';
CREATE DATABASE combo_db OWNER combo;
\c combo_db
CREATE EXTENSION IF NOT EXISTS pg_trgm;
\q
```

### 3) Python environment
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```

### 4) Configure `.env`
```bash
cp .env.example .env
```
Set:
```env
DATABASE_URL=postgresql+asyncpg://combo:combo@localhost:5432/combo_db
REDIS_URL=redis://localhost:6379/0
CELERY_BROKER_URL=redis://localhost:6379/1
CELERY_RESULT_BACKEND=redis://localhost:6379/2
BASIC_AUTH_USERNAME=admin
BASIC_AUTH_PASSWORD=change-this-password
```

### 5) Run app
Terminal 1:
```bash
source .venv/bin/activate
uvicorn app.main:app --host 0.0.0.0 --port 8000
```

Terminal 2:
```bash
source .venv/bin/activate
celery -A app.celery_app.celery_app worker --loglevel=INFO
```

Optional terminal 3:
```bash
source .venv/bin/activate
celery -A app.celery_app.celery_app flower --port=5555
```

### 6) File path format in UI
Use absolute macOS paths:
- Input folder: `/Users/<you>/combo/input`
- Output file: `/Users/<you>/combo/output/cleaned_format.txt`

## Linux Native Setup (No Docker)
Example uses Ubuntu/Debian.

### 1) Install dependencies
```bash
sudo apt update
sudo apt install -y python3 python3-venv python3-pip build-essential libpq-dev postgresql-15 postgresql-client-15 redis-server
sudo systemctl enable --now postgresql
sudo systemctl enable --now redis-server
```

### 2) Create DB/user/extension
```bash
sudo -u postgres psql
```
Run:
```sql
CREATE ROLE combo WITH LOGIN PASSWORD 'combo';
CREATE DATABASE combo_db OWNER combo;
\c combo_db
CREATE EXTENSION IF NOT EXISTS pg_trgm;
\q
```

### 3) Python environment
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```

### 4) Configure `.env`
```bash
cp .env.example .env
```
Set:
```env
DATABASE_URL=postgresql+asyncpg://combo:combo@localhost:5432/combo_db
REDIS_URL=redis://localhost:6379/0
CELERY_BROKER_URL=redis://localhost:6379/1
CELERY_RESULT_BACKEND=redis://localhost:6379/2
BASIC_AUTH_USERNAME=admin
BASIC_AUTH_PASSWORD=change-this-password
```

### 5) Run app
Terminal 1:
```bash
source .venv/bin/activate
uvicorn app.main:app --host 0.0.0.0 --port 8000
```

Terminal 2:
```bash
source .venv/bin/activate
celery -A app.celery_app.celery_app worker --loglevel=INFO
```

Optional terminal 3:
```bash
source .venv/bin/activate
celery -A app.celery_app.celery_app flower --port=5555
```

### 6) File path format in UI
Use absolute Linux paths:
- Input folder: `/home/<you>/combo/input`
- Output file: `/home/<you>/combo/output/cleaned_format.txt`

## First-Time Operation Checklist
1. Open `http://localhost:8000`.
2. Log in with Basic Auth values from `.env`.
3. Start a Cleaner job.
4. Wait for completion.
5. Start an Upload job using the cleaned output file.
6. Open Search page and test a query.

## Troubleshooting
### App does not open
- Check web process is running.
- Check port 8000 is free.

### Jobs stay in queued
- Worker is not running or cannot reach Redis.
- Check worker terminal logs.

### Database connection fails
- PostgreSQL service not running.
- Wrong `DATABASE_URL`.
- Role/database not created.

### Search is slow
- Very broad queries can be slower.
- Use more specific filters.
- Ensure PostgreSQL has enough RAM and SSD storage.
