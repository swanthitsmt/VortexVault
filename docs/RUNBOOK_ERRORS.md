# VortexVault v2 Error Runbook

## 1) `invalid byte sequence for encoding "UTF8": 0x00`
Cause:
- Raw data contains NUL bytes.

Fix:
- Parser already strips `\x00` in `backend/vortexvault/services/parser.py`.
- Restart failed ingest job via resume endpoint.
- If corruption is extreme, re-upload sanitized source object.

## 2) Search latency spikes / timeout feeling
Cause:
- Broad query across all shards.
- High ingest pressure while searching.

Fix order:
1. Narrow filters (`filter_url` / `filter_username`).
2. Lower `limit`.
3. Scale down ingest concurrency temporarily.
4. Check Meili shard health and NVMe saturation.

## 3) Upload fast, ingest slow
Cause:
- Search shard indexing saturation.
- Redis or DB backpressure.

Fix:
- Reduce `INGEST_BATCH_DOCS` (ex: `25000 -> 15000`) for smoother commits.
- Tune ingest worker count based on CPU/IO.
- Confirm Meili shards on dedicated NVMe.

## 4) Merge seems idle
Cause:
- Merge queue is intentionally serial (`concurrency=1`) and only runs after ingest completion.

Check:
```bash
docker compose logs worker-merge --tail=200
```

## 5) Export delayed
Cause:
- Very broad query with large line limit.

Fix:
- Run export with tighter filters.
- Keep export worker isolated from ingest.
- Increase export workers only if search shards still have headroom.

## 6) Resume not continuing correctly
Cause:
- Source object key changed or replaced after checkpoint.

Fix:
- Keep immutable object keys per run.
- Start a new ingest job for new object versions.
