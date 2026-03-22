# Performance SLO and Preflight Gates

## Workload Target
- Concurrent users: 10-30
- Daily ingest: 20-100 GB
- DB size horizon: 1-5 TB

## Service Goals
- Search p95 latency < 300 ms (filtered queries under target load)
- No stuck jobs in ingest/merge pipeline
- Export line-limit is exact and downloadable

## Preflight Matrix (local production-like)
1. Ingest tests:
- 2 GB dataset
- 20 GB dataset

2. Search tests:
- 10 concurrent users
- 20 concurrent users

3. Export tests:
- 100k line export
- 1M line export

## Pass Criteria
- Pipeline completes with cleanup (no staging/checkpoint leftovers)
- Search performance remains stable without sustained timeout spikes
- Export job completes within configured timeout window
- Restart/retry resumes without duplicate inflation
