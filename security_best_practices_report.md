# VortexVault v2 Security Best Practices Report

Date: 2026-03-26  
Scope: `backend/`, `frontend/`, `docker-compose*.yml`, `.env*`, setup docs  
Method: static review + tool-based scanning + limited local checks

## Executive Summary
- Baseline security posture is better than before: input validation exists for object keys/buckets, Meili filter escaping exists, optional API bearer auth exists, and security headers are applied.
- New hardening was added in this review: production startup now checks weak placeholders, Flower now supports optional basic auth, and Flower/MinIO console default bind is loopback.
- No dependency vulnerabilities were reported by `npm audit` for frontend prod dependencies.
- A full runtime pentest is **not completed** in this environment because Docker daemon was unavailable during this run.

## Validation Evidence
- `python3 -m compileall backend/vortexvault` -> pass
- `npm run build` -> pass
- `npm audit --omit=dev --json` -> 0 vulnerabilities
- `bandit -r backend/vortexvault` -> 2 warnings (bind all interfaces, temp dir usage)
- Schema/security smoke checks (local Python) -> 6/6 pass
  - path traversal rejection
  - bucket format rejection
  - query max length enforcement
  - object/bucket sanitizer checks
  - Meili filter escaping check

## Findings

### High

#### F-001: Weak default credentials still exist in compose/env fallbacks (operator misconfiguration risk)
- Rule ID: FASTAPI-SUPPLY-001 / Deployment secret hygiene
- Severity: High
- Location:
  - `docker-compose.yml:33-46`
  - `docker-compose.lite.yml:32-45`
  - `.env.local:12-24`
  - `.env.lite:12-24`
- Evidence:
  - default values like `POSTGRES_PASSWORD=vortexvault`, `MINIO_ROOT_PASSWORD=vortexvault123`, `MEILI_MASTER_KEY=vortexvault-master-key`.
- Impact:
  - If deployed with defaults, DB/object/search planes can be compromised quickly on LAN.
- Fix:
  - Keep strong unique secrets in `.env` before deployment.
  - Keep `APP_ENV=prod` for strict startup rejection of weak values.
- Mitigation:
  - Restrict network exposure to LAN/VPN only.
  - Rotate all credentials on each environment.

### Medium

#### F-002: API auth is optional, and frontend client has no Authorization header support
- Rule ID: FASTAPI-AUTH-001
- Severity: Medium
- Location:
  - `backend/vortexvault/config.py:16-18,67-73`
  - `frontend/src/api.ts:71-99`
- Evidence:
  - API token can be empty; frontend fetch wrapper never injects bearer token.
- Impact:
  - Teams may disable API auth to keep UI working, lowering security posture.
- Fix:
  - Add UI auth flow (session or bearer input) and include `Authorization` in API wrapper.
- Mitigation:
  - Keep API behind LAN reverse proxy auth when token mode is not used in UI.

#### F-003: No rate limiting on costly endpoints (search/export/ingest create)
- Rule ID: FASTAPI-DOS-001
- Severity: Medium
- Location:
  - `backend/vortexvault/main.py:154-290`
- Evidence:
  - No request throttle guard on ingest/search/export endpoints.
- Impact:
  - Burst traffic can degrade worker/search responsiveness (application-level DoS risk).
- Fix:
  - Add Redis-backed token bucket per IP/user for `/api/v2/search/query`, `/api/v2/exports`, `/api/v2/ingest/jobs`.
- Mitigation:
  - Enforce upstream NGINX rate limits until app-level limiter is shipped.

### Low

#### F-004: API binds all interfaces
- Rule ID: FASTAPI-DEPLOY-001 (context-dependent)
- Severity: Low
- Location:
  - `backend/vortexvault/config.py:14`
  - `docker-compose*.yml` API command lines (`--host 0.0.0.0`)
- Evidence:
  - `0.0.0.0` bind is used for container networking.
- Impact:
  - Expected in containerized setups, but dangerous if network boundaries are weak.
- Fix:
  - Keep only edge port published; avoid direct API host port publication.

#### F-005: Export temp files use writable local path
- Rule ID: FASTAPI-FILES-001 (operational hardening)
- Severity: Low
- Location:
  - `backend/vortexvault/config.py:46`
  - `backend/vortexvault/services/export_pipeline.py:29-33,112-113`
- Evidence:
  - Export job writes parquet temp file to configured tmp path before upload.
- Impact:
  - If host permissions are too broad, local user snooping risk increases.
- Fix:
  - Use dedicated restricted directory/volume with tight permissions.

## Hardening Applied In This Review
1. Added runtime configuration checks:
   - `backend/vortexvault/security.py`
   - `backend/vortexvault/main.py`
2. Added optional Flower basic auth and safer default bind addresses:
   - `docker-compose.yml`, `docker-compose.lite.yml`
   - `.env.example`, `.env.local`, `.env.lite`, `.env.prodlocal`
3. Updated docs for the new hardening vars:
   - `README.md`
   - `docs/SETUP_SERVER_PRODUCTION_MM.md`
   - `docs/SETUP_LOCAL_LITE_MM.md`

## Pentest Coverage Status
- Code-level and config-level review: completed for scoped files above.
- Tool-assisted scanning: partially completed (`npm audit`, `bandit`).
- Runtime penetration test (auth bypass, SSRF probes, API abuse, queue abuse): not completed in this run due to Docker daemon unavailable.

## Recommended Next Pentest Steps
1. Start stack and run authenticated/unauthenticated endpoint matrix tests.
2. Run dynamic scans against edge endpoint (ZAP/Burp active checks in controlled profile).
3. Execute load-abuse scenarios (search/export flood, queue starvation, replayed ingest jobs).
4. Validate credential rotation and secret-injection workflow on real deployment target.
