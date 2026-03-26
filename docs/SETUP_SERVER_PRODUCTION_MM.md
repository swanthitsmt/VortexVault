# VortexVault Server Setup Guide (Production, Proxmox)

ဒီ guide က Proxmox server ပေါ်မှာ VortexVault ကို production-style run ဖို့ အဆင့်လိုက် command-by-command guide ဖြစ်ပါတယ်။

## 1) Proxmox VM Prepare
အကြံပြု VM spec:
- CPU: 24-32 vCPU
- RAM: 64GB-128GB
- Disk: NVMe (prefer dedicated volume)
- OS: Ubuntu 24.04 LTS

VM ထဲ SSH ဝင်ပြီး:
```bash
ssh <user>@<server-ip>
```

## 2) System Packages
```bash
sudo apt update
sudo apt -y upgrade
sudo apt -y install ca-certificates curl gnupg lsb-release git jq
```

## 3) Docker Engine + Compose Plugin Install
```bash
sudo install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
sudo chmod a+r /etc/apt/keyrings/docker.gpg

echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu \
  $(. /etc/os-release && echo $VERSION_CODENAME) stable" | \
  sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

sudo apt update
sudo apt -y install docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
```

docker user permission:
```bash
sudo usermod -aG docker $USER
newgrp docker
```

verify:
```bash
docker version
docker compose version
```

## 4) Repo Clone
```bash
cd ~
git clone https://github.com/swanthitsmt/VortexVault.git
cd VortexVault
```

## 5) Production Env Prepare
```bash
cp .env.prodlocal .env
nano .env
```

`.env` မှာ အနည်းဆုံးပြင်ရန်:
- `POSTGRES_PASSWORD`
- `MINIO_ROOT_PASSWORD`
- `MEILI_MASTER_KEY`
- `API_AUTH_TOKEN` (အရေးကြီး, random long token)
- `FLOWER_BASIC_AUTH` (example `ops:<strong-password>`)
- `FLOWER_BIND_ADDR=127.0.0.1` (default)
- `MINIO_CONSOLE_BIND_ADDR=127.0.0.1` (default)
- `EDGE_BIND_PORT` (လိုသလို)

## 6) Start Stack
```bash
docker compose -f docker-compose.yml -f docker-compose.prodlocal.yml up -d --build
```

status စစ်:
```bash
docker compose ps
```

logs စစ်:
```bash
docker compose logs -f api
docker compose logs -f worker-ingest
```

## 7) Health Check
```bash
curl -s http://127.0.0.1:8000/health
curl -s -H "Authorization: Bearer <API_AUTH_TOKEN>" http://127.0.0.1:8000/api/v2/dashboard | jq
```

## 8) Firewall (LAN Only)
UFW သုံးမယ်ဆို:
```bash
sudo ufw allow 22/tcp
sudo ufw allow 8000/tcp
sudo ufw deny 9001/tcp
sudo ufw deny 5555/tcp
sudo ufw --force enable
sudo ufw status
```

## 9) Basic Smoke Test (Ingest + Search)
sample file create:
```bash
cat > /tmp/vortex_sample.txt <<'TXT'
https://example.com,user1,pass1
https://gmail.com,user2,pass2
TXT
```

presign request:
```bash
RESP=$(curl -sS -X POST http://127.0.0.1:8000/api/v2/files/presign \
  -H "Authorization: Bearer <API_AUTH_TOKEN>" \
  -H 'Content-Type: application/json' \
  -d '{"object_name":"sample/vortex_sample.txt"}')
echo "$RESP" | jq
```

upload:
```bash
PUT_URL=$(echo "$RESP" | jq -r '.put_url')
OBJECT_KEY=$(echo "$RESP" | jq -r '.object_key')
curl -sS -X PUT --upload-file /tmp/vortex_sample.txt "$PUT_URL"
```

ingest job create:
```bash
JOB=$(curl -sS -X POST http://127.0.0.1:8000/api/v2/ingest/jobs \
  -H "Authorization: Bearer <API_AUTH_TOKEN>" \
  -H 'Content-Type: application/json' \
  -d "{\"source_bucket\":\"raw-combos\",\"source_object\":\"$OBJECT_KEY\",\"auto_merge\":true}")
echo "$JOB" | jq
JOB_ID=$(echo "$JOB" | jq -r '.id')
```

poll:
```bash
watch -n 2 "curl -s -H 'Authorization: Bearer <API_AUTH_TOKEN>' http://127.0.0.1:8000/api/v2/ingest/jobs/$JOB_ID | jq '{status,processed_lines,indexed_docs,duplicate_lines}'"
```

search:
```bash
curl -sS -X POST http://127.0.0.1:8000/api/v2/search/query \
  -H "Authorization: Bearer <API_AUTH_TOKEN>" \
  -H 'Content-Type: application/json' \
  -d '{"query":"gmail.com","limit":20,"prefix":true,"typo_tolerance":true}' | jq
```

## 10) Restart / Update Workflow
```bash
cd ~/VortexVault
git pull origin main
docker compose -f docker-compose.yml -f docker-compose.prodlocal.yml up -d --build
```

## 11) Stop / Start / Full Down
stop only:
```bash
docker compose stop
```

start only:
```bash
docker compose start
```

full down (container only, volume မဖျက်):
```bash
docker compose down
```

full down + volumes (data wipe):
```bash
docker compose down -v
```

## 12) Backup (Quick)
PostgreSQL metadata backup:
```bash
docker compose exec -T postgres pg_dump -U vortexvault -d vortexvault > backup_meta.sql
```

MinIO object backup (host-level):
```bash
docker run --rm -v minio_data:/data -v $PWD:/backup alpine tar czf /backup/minio_backup.tgz -C /data .
```
