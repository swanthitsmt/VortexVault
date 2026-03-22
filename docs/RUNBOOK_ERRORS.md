# Error Runbook

## 1) `invalid byte sequence for encoding "UTF8": 0x00`
Cause:
- NUL byte in source line during COPY into PostgreSQL.

Fix:
1. Ensure parser/COPY sanitizer removes `\x00`.
2. Re-run failed upload job.
3. Confirm new rows are flowing into staging.

## 2) `Search timed out`
Cause:
- Broad filters, expensive exact count, or index mismatch.

Fix order:
1. Disable exact total for wide queries.
2. Narrow filters.
3. Increase `SEARCH_STATEMENT_TIMEOUT_MS` and `SEARCH_COUNT_TIMEOUT_MS`.
4. Verify trigram indexes exist and are valid.

## 3) Upload slow + merge lag
Cause:
- WAL/checkpoint pressure or DB I/O saturation.

Fix:
1. Temporarily reduce upload workers.
2. Increase `PG_MAX_WAL_SIZE`, tune checkpoint settings.
3. Confirm Postgres on dedicated NVMe and healthy IOPS.
4. Keep merge workers isolated from upload workers.

## 4) ES drift / empty first page
Cause:
- Elasticsearch sync lag.

Fix:
1. Keep PostgreSQL fallback enabled.
2. Monitor sync ratio and ES document count.
3. Trigger backfill sync.

## 5) CSV export too slow or timeout
Cause:
- Heavy synchronous stream export.

Fix:
1. Use async export jobs for large limits.
2. Use SQL COPY + gzip output.
3. Apply TTL cleanup for old export artifacts.
