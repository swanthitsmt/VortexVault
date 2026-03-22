# Massive Combo List Cleaner

> Status: `v2 active development` for stream-first ingest, shard-parallel merge, and production-local preflight workflows.

High-performance FastAPI web app for cleaning huge combo files, uploading to PostgreSQL, and searching/exporting data.

## What This App Does
1. Reads very large `.txt` combo files from a folder.
2. Cleans and normalizes lines to `url,username,password`.
3. Removes duplicates in streaming mode.
4. Uploads cleaned data to PostgreSQL in high-throughput batches.
5. Provides web search with pagination and CSV export.

## Main Components
- FastAPI web server
- Celery worker
- Redis broker/backend
- PostgreSQL 15+
- Bootstrap web UI

## Setup Guides
- Docker setup (recommended for most users): [docs/LOCAL_SETUP.md](docs/LOCAL_SETUP.md#docker-setup-recommended)
- Native Windows setup: [docs/LOCAL_SETUP.md](docs/LOCAL_SETUP.md#windows-native-setup-no-docker)
- Native macOS setup: [docs/LOCAL_SETUP.md](docs/LOCAL_SETUP.md#macos-native-setup-no-docker)
- Native Linux setup: [docs/LOCAL_SETUP.md](docs/LOCAL_SETUP.md#linux-native-setup-no-docker)
- GitHub private repository guide: [docs/GITHUB_PRIVATE_REPO.md](docs/GITHUB_PRIVATE_REPO.md)

## Quick Start (Docker)
```bash
cp .env.example .env
./scripts/one_click_setup.sh
```

Open:
- App: `http://localhost:8000`
- Flower: `http://localhost:5555`

Default Basic Auth (change in `.env`):
- Username: `admin`
- Password: `admin123`

## v2 Environment Profiles
- Local development profile: `.env.local` + `docker-compose.local.yml`
- Production-local (single-host simulation): `.env.prodlocal` + `docker-compose.prodlocal.yml`

Run local profile:
```bash
cp .env.local .env
docker compose -f docker-compose.yml -f docker-compose.local.yml up -d --build
```

Run prodlocal profile:
```bash
cp .env.prodlocal .env
docker compose -f docker-compose.yml -f docker-compose.prodlocal.yml up -d --build
```

## Default Workflow
1. Start a Cleaner job from dashboard.
2. Wait for completion (or pause/resume).
3. Start an Upload job using cleaned output file.
4. Search and export from the Search page.

## Path Rules
- Docker mode: use container paths like `/app/data/input`.
- Native mode: use host absolute paths like `C:\...`, `/Users/...`, `/home/...`.

## Security Notes
- Never commit `.env`.
- Never commit raw combo files or cleaned exports.
- Use strong Basic Auth credentials in production.
- Run behind HTTPS reverse proxy in real deployment.
