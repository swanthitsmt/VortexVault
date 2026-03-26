# VortexVault Local Lite Guide (Low Resource PC)

ဒီ guide က laptop / desktop (RAM 8GB-16GB) အတွက် lightweight mode နဲ့ run စမ်းဖို့ပါ။

## 1) Requirements
- Docker Desktop installed
- Git installed
- CPU virtualization enabled

verify:
```bash
docker version
docker compose version
git --version
```

## 2) Clone
```bash
cd ~/Desktop
git clone https://github.com/swanthitsmt/VortexVault.git
cd VortexVault
```

## 3) Lite Profile Files
ဒီ repo မှာ lite files ပါပြီးသား:
- `.env.lite`
- `docker-compose.lite.yml`
- `scripts/lite_up.sh`
- `scripts/lite_down.sh`

## 4) Start Lite Stack (One Command)
```bash
./scripts/lite_up.sh
```

manual start လုပ်ချင်ရင်:
```bash
cp .env.lite .env
docker compose -f docker-compose.lite.yml up -d --build
```

## 5) URLs
- UI: `http://localhost:8000`
- API docs: `http://localhost:8000/docs`
- Flower: `http://localhost:5556`
- MinIO Console: `http://localhost:9002`

## 6) Health Check
```bash
curl -s http://localhost:8000/health
curl -s http://localhost:8000/api/v2/dashboard
```

## 7) Small Data End-to-End Test
### 7.1 test txt create
```bash
cat > /tmp/vortex_local_test.txt <<'TXT'
https://facebook.com,alice@example.com,pass123
https://gmail.com,bob@gmail.com,qwerty
https://github.com,charlie@dev.io,zxc123
TXT
```

### 7.2 presigned upload URL request
```bash
RESP=$(curl -sS -X POST http://localhost:8000/api/v2/files/presign \
  -H 'Content-Type: application/json' \
  -d '{"object_name":"lite/vortex_local_test.txt"}')

echo "$RESP"
```

Python နဲ့ parse:
```bash
PUT_URL=$(python3 - <<'PY' "$RESP"
import json,sys
print(json.loads(sys.argv[1])["put_url"])
PY
)
OBJECT_KEY=$(python3 - <<'PY' "$RESP"
import json,sys
print(json.loads(sys.argv[1])["object_key"])
PY
)
```

### 7.3 upload to MinIO
```bash
curl -sS -X PUT --upload-file /tmp/vortex_local_test.txt "$PUT_URL"
```

### 7.4 create ingest job
```bash
JOB=$(curl -sS -X POST http://localhost:8000/api/v2/ingest/jobs \
  -H 'Content-Type: application/json' \
  -d "{\"source_bucket\":\"raw-combos\",\"source_object\":\"$OBJECT_KEY\",\"auto_merge\":true}")

echo "$JOB"
```

job id extract:
```bash
JOB_ID=$(python3 - <<'PY' "$JOB"
import json,sys
print(json.loads(sys.argv[1])["id"])
PY
)

echo "$JOB_ID"
```

### 7.5 wait until complete
```bash
while true; do
  STATE=$(curl -sS "http://localhost:8000/api/v2/ingest/jobs/$JOB_ID")
  STATUS=$(python3 - <<'PY' "$STATE"
import json,sys
print(json.loads(sys.argv[1])["status"])
PY
)
  echo "status=$STATUS"
  if [ "$STATUS" = "completed" ] || [ "$STATUS" = "failed" ]; then
    echo "$STATE"
    break
  fi
  sleep 2
done
```

### 7.6 search test
```bash
curl -sS -X POST http://localhost:8000/api/v2/search/query \
  -H 'Content-Type: application/json' \
  -d '{"query":"gmail.com","limit":20,"prefix":true,"typo_tolerance":true}'
```

## 8) Stop Lite Stack
```bash
./scripts/lite_down.sh
```

## 9) Start Again
```bash
./scripts/lite_up.sh
```

## 10) Clean Reset (all data wipe)
```bash
docker compose -f docker-compose.lite.yml down -v
docker volume prune -f
```

## 11) Troubleshooting
container မတက်ရင်:
```bash
docker compose -f docker-compose.lite.yml ps
docker compose -f docker-compose.lite.yml logs --tail=200 api
docker compose -f docker-compose.lite.yml logs --tail=200 worker-ingest
```

docker daemon error ရင်:
- Docker Desktop app ကိုဖွင့်ပြီး wait until "Engine running" ဖြစ်မှ command ပြန်ရိုက်ပါ။
