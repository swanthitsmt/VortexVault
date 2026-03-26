# VortexVault v2 Deployment on Single Proxmox Host

## Target
Single Proxmox node with:
- 32+ cores
- 128GB+ RAM
- NVMe storage
- LAN-only access

## Recommended Topology
- VM/LXC-1: `edge` (NGINX)
- VM/LXC-2: `app` (FastAPI + Celery workers)
- VM/LXC-3: `db` (PostgreSQL)
- VM/LXC-4: `queue-storage` (Redis Stack + MinIO)
- VM/LXC-5..8: `search-shard-0..3` (Meilisearch)

For first production rollout, a single VM can host all containers using `docker-compose.yml`.

## Storage Layout
Use separate virtual disks/LVM volumes:
1. `/var/lib/vortexvault/postgres` -> PostgreSQL
2. `/var/lib/vortexvault/minio` -> MinIO
3. `/var/lib/vortexvault/meili0` ... `/meili3` -> each shard

## Network
- Keep services on internal bridge network.
- Expose only edge `:80/:443` to LAN.
- Restrict MinIO console and Flower to admin VLAN or VPN.

## Install
```bash
git clone https://github.com/swanthitsmt/VortexVault-Search-v2.git
cd VortexVault-Search-v2
cp .env.prodlocal .env
```

Edit `.env` for production secrets and host-specific values:
- `POSTGRES_PASSWORD`
- `MINIO_ROOT_PASSWORD`
- `MEILI_MASTER_KEY`
- worker concurrency knobs

## Launch
```bash
docker compose -f docker-compose.yml -f docker-compose.prodlocal.yml up -d --build
```

## Validate
```bash
curl -s http://<host-ip>:8000/health
curl -s http://<host-ip>:8000/api/v2/dashboard
```

## Cutover Checklist
- [ ] ingest test object runs to completion
- [ ] merge auto-run works
- [ ] search latency under expected target on hot queries
- [ ] export job creates downloadable parquet
- [ ] restart of `api` and workers resumes correctly

## Hardening
- Add TLS certificate on edge NGINX.
- Restrict management endpoints.
- Enable automatic backups:
  - PostgreSQL daily dump/snapshot
  - MinIO bucket replication or periodic backup
- Add node-level monitoring (CPU, RAM, disk IOPS, latency)
