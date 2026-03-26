# Meilisearch Sharding Strategy

## Shard Routing
FluxDB v2 uses hash-based routing by digest:

```python
shard_id = int(digest_hex[:8], 16) % shard_count
```

This guarantees deterministic placement per row and enables idempotent retries.

## Index Naming
- `combo_s00`
- `combo_s01`
- `combo_s02`
- `combo_s03`

Defined in `backend/fluxdb/services/meili.py`.

## Index Settings
Canonical settings file:
- `backend/fluxdb/services/configs/meili_index_settings.json`

Applied on API/worker startup via `meili_router.ensure_indexes()`.

## Federated Search
For each query:
1. Send parallel search requests to all shards.
2. Collect hits and ranking score.
3. Merge + score-sort.
4. Return top N globally.

## Operational Notes
- Add more shards only when per-shard CPU/IO is saturated.
- Keep each shard on separate NVMe path when possible.
- Keep `MEILI_MASTER_KEY` identical across shards.
