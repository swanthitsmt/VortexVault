# Performance Optimization Guide

## Cleaner Pipeline
- Reads each source file as a stream; no full-file memory load.
- Persists dedupe set in SQLite with WAL mode.
- Flush interval (`CLEANER_FLUSH_EVERY`) controls checkpoint frequency and IO pressure.
- Pause/resume works via checkpointed text stream position and per-file line counter.

## Upload Pipeline
- Reads cleaned file as a stream.
- Batch size (`UPLOAD_BATCH_SIZE`) controls throughput.
- Uses PostgreSQL `COPY` into temp staging table.
- Final dedupe insert uses:
  - `ON CONFLICT (digest, url, username, password) DO NOTHING`

## Database Tuning (PostgreSQL)
Recommended for large ingest workloads:
- `shared_buffers`: 25% RAM
- `work_mem`: tune per connection
- `maintenance_work_mem`: large enough for index builds
- `max_wal_size`: increase for large copy jobs
- Use fast SSD/NVMe storage

For very large datasets (100M+), consider:
- Table partitioning by hash/range
- Read replicas for heavy search traffic
- Periodic `VACUUM (ANALYZE)`
- `autovacuum` tuning

## Search Performance
- `pg_trgm` extension enabled.
- GIN trigram indexes on URL/username/password.
- B-Tree composite index for exact lookup paths.
- Regex queries can still be expensive for broad patterns.

## Expected Hotspots
- First-time dedupe for huge source folders is disk-heavy.
- Deep pagination (very high page numbers) can be slower.
- Regex on broad patterns can bypass optimal index usage.

## Practical Recommendations
- Prefer specific filters over empty queries.
- Export targeted datasets instead of full table dumps.
- Keep worker and DB close in network topology.
