# Deploy on Proxmox (Single Host, LAN-Only TLS)

## Target Shape
- `edge` (Nginx TLS): 2 vCPU / 2-4 GB RAM
- `app` (FastAPI + workers): 8 vCPU / 16 GB RAM
- `db` (PostgreSQL): 12-16 vCPU / 48-64 GB RAM / dedicated NVMe
- `search` (Elasticsearch): 8 vCPU / 24-32 GB RAM / dedicated NVMe
- `queue` (Redis + Flower): 2 vCPU / 4 GB RAM

## Network
- Place all services in private VLAN.
- Expose only `edge:443` to LAN users.
- Do not expose DB/Redis/ES ports to LAN.

## Bootstrap
1. Clone this repository on app host.
2. Copy `.env.prodlocal` to `.env` and set strong passwords.
3. Generate local TLS certs:
   - `./scripts/generate_local_tls_cert.sh`
4. Start stack:
   - `docker compose -f docker-compose.yml -f docker-compose.prodlocal.yml up -d --build`
5. Verify health:
   - `docker compose ps`
   - Open `https://<host-ip>/dashboard`

## Storage Guidance
- Use dedicated virtual disk for Postgres data.
- Use separate virtual disk for Elasticsearch.
- Keep backups on separate storage/NAS.

## Backup & Recovery
- Nightly DB full backup + WAL archive.
- Keep at least 7 daily + 4 weekly restore points.
- Test restore on staging VM monthly.
