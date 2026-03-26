from __future__ import annotations

import logging
import re
import secrets
from collections.abc import Iterable

from fastapi import HTTPException, Request, status
from fastapi.responses import JSONResponse

from vortexvault.config import settings

# Conservative allow-list to avoid odd object keys and traversal semantics.
_ALLOWED_OBJECT_KEY = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._/\-]{1,510}[A-Za-z0-9]$")
_WEAK_API_TOKENS = {"", "change-me", "changeme", "change-this-long-random-token"}
_WEAK_MEILI_KEYS = {"", "change-me", "changeme", "vortexvault-master-key"}
_WEAK_MINIO_PASSWORDS = {"", "minioadmin", "vortexvault123"}

logger = logging.getLogger(__name__)


def sanitize_object_name(object_name: str) -> str:
    value = object_name.strip()
    if not _ALLOWED_OBJECT_KEY.match(value):
        raise HTTPException(status_code=422, detail="Invalid object name format")
    if ".." in value or "//" in value or "\\" in value:
        raise HTTPException(status_code=422, detail="Invalid object name path segments")
    return value


def sanitize_bucket_name(bucket_name: str) -> str:
    value = bucket_name.strip()
    if not value or len(value) > 63:
        raise HTTPException(status_code=422, detail="Invalid bucket name")
    if not re.fullmatch(r"[a-z0-9][a-z0-9.-]*[a-z0-9]", value):
        raise HTTPException(status_code=422, detail="Invalid bucket name format")
    return value


def escape_meili_filter(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', '\\"').replace("\n", " ").replace("\r", " ")


def is_exempt_path(path: str, exempt_paths: Iterable[str]) -> bool:
    for exempt in exempt_paths:
        if path == exempt or path.startswith(f"{exempt}/"):
            return True
    return False


async def auth_middleware(request: Request, call_next):
    if settings.is_auth_enabled and not is_exempt_path(request.url.path, settings.auth_exempt_paths):
        auth_header = request.headers.get("Authorization", "")
        if not auth_header.startswith("Bearer "):
            return JSONResponse(status_code=status.HTTP_401_UNAUTHORIZED, content={"detail": "Missing bearer token"})

        token = auth_header.removeprefix("Bearer ").strip()
        if not secrets.compare_digest(token, settings.api_auth_token):
            return JSONResponse(status_code=status.HTTP_403_FORBIDDEN, content={"detail": "Invalid bearer token"})

    response = await call_next(request)
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["Referrer-Policy"] = "no-referrer"
    response.headers["Permissions-Policy"] = "camera=(), microphone=(), geolocation=()"
    return response


def validate_runtime_security_or_raise() -> None:
    issues: list[str] = []

    token = settings.api_auth_token.strip()
    if token in _WEAK_API_TOKENS:
        issues.append("API_AUTH_TOKEN is empty or weak placeholder")

    meili_key = settings.meili_master_key.strip()
    if meili_key in _WEAK_MEILI_KEYS:
        issues.append("MEILI_MASTER_KEY is empty or weak placeholder")

    minio_secret = settings.minio_secret_key.strip()
    if minio_secret in _WEAK_MINIO_PASSWORDS:
        issues.append("MINIO_ROOT_PASSWORD is empty or weak default")

    # In strict production, fail fast. In local/prodlocal/lite, warn loudly.
    if issues and settings.app_env.lower() in {"prod", "production"}:
        raise RuntimeError("Insecure production configuration: " + "; ".join(issues))

    for issue in issues:
        logger.warning("Security warning: %s", issue)
