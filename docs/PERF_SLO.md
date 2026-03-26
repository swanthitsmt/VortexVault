# VortexVault v2 Performance SLO (Target)

## Search SLO
- P50: <= 25ms (hot queries)
- P95: <= 50ms (hot queries)
- P99: <= 150ms

## Ingest SLO
- Sustained ingest speed should not stall for > 60s without checkpoint updates.
- Checkpoint update interval should remain predictable under configured stride (`10-50GB`).
- Resume should continue from last successful checkpoint.

## Export SLO
- 100k rows export: typically <= 30s under normal load.
- 1M rows export: typically <= 120s depending on selectivity.

## Resource SLO
- PostgreSQL disk usage should stay metadata-dominant (small compared to raw dataset).
- MinIO and Meili hold the bulk of data.
- No unbounded queue growth for > 5 minutes.

## Measurement Commands
Search load sample:
```bash
./scripts/search_load_test.sh gmail.com 200 20 http://localhost:8000
```

Ingest benchmark sample:
```bash
./scripts/benchmark_matrix.sh raw-combos raw/2gb.txt raw/20gb.txt http://localhost:8000
```

Export verification sample:
```bash
./scripts/export_verify.sh gmail.com 100000 /tmp/export.parquet http://localhost:8000
```
