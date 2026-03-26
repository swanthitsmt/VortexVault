from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any

import httpx

from vortexvault.config import settings
from vortexvault.security import escape_meili_filter

_SETTINGS_PATH = Path(__file__).resolve().parent / "configs" / "meili_index_settings.json"
INDEX_SETTINGS = json.loads(_SETTINGS_PATH.read_text(encoding="utf-8"))


class MeiliShardRouter:
    def __init__(self) -> None:
        self.hosts = settings.meili_hosts
        self.master_key = settings.meili_master_key
        self.index_prefix = settings.meili_index_prefix

    @property
    def shard_count(self) -> int:
        return len(self.hosts)

    def shard_for_digest(self, digest_hex: str) -> int:
        return int(digest_hex[:8], 16) % self.shard_count

    def index_name(self, shard_id: int) -> str:
        return f"{self.index_prefix}_s{shard_id:02d}"

    def _headers(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {self.master_key}"}

    async def ensure_indexes(self) -> None:
        headers = self._headers()
        timeout = httpx.Timeout(20.0, read=20.0)
        async with httpx.AsyncClient(timeout=timeout) as client:
            for shard_id, host in enumerate(self.hosts):
                index_uid = self.index_name(shard_id)
                await client.post(f"{host}/indexes", headers=headers, json={"uid": index_uid, "primaryKey": "id"})
                await client.patch(f"{host}/indexes/{index_uid}/settings", headers=headers, json=INDEX_SETTINGS)

    async def index_documents(self, shard_id: int, docs: list[dict[str, Any]]) -> None:
        if not docs:
            return
        headers = self._headers()
        host = self.hosts[shard_id]
        index_uid = self.index_name(shard_id)
        timeout = httpx.Timeout(30.0, read=30.0)
        async with httpx.AsyncClient(timeout=timeout) as client:
            response = await client.post(f"{host}/indexes/{index_uid}/documents", headers=headers, json=docs)
            response.raise_for_status()

    async def search_shard(
        self,
        *,
        shard_id: int,
        query: str,
        limit: int,
        offset: int,
        filter_url: str | None,
        filter_username: str | None,
        prefix: bool,
        typo_tolerance: bool,
    ) -> list[dict[str, Any]]:
        host = self.hosts[shard_id]
        index_uid = self.index_name(shard_id)
        headers = self._headers()

        filter_terms: list[str] = []
        if filter_url:
            filter_terms.append(f'url CONTAINS "{escape_meili_filter(filter_url)}"')
        if filter_username:
            filter_terms.append(f'username CONTAINS "{escape_meili_filter(filter_username)}"')

        payload: dict[str, Any] = {
            "q": query,
            "limit": limit,
            "offset": offset,
            "showRankingScore": True,
            "attributesToRetrieve": ["id", "url", "username", "password", "digest", "ingested_at", "shard"],
            "attributesToCrop": [],
            "attributesToHighlight": [],
            "matchingStrategy": "last" if prefix else "all",
            "typoTolerance": typo_tolerance,
        }
        if filter_terms:
            payload["filter"] = " AND ".join(filter_terms)

        timeout = httpx.Timeout(settings.meili_search_timeout_ms / 1000.0, read=settings.meili_search_timeout_ms / 1000.0)
        async with httpx.AsyncClient(timeout=timeout) as client:
            response = await client.post(f"{host}/indexes/{index_uid}/search", headers=headers, json=payload)
            response.raise_for_status()
        data = response.json()
        hits: list[dict[str, Any]] = data.get("hits", [])
        for row in hits:
            row["shard"] = shard_id
            row["score"] = float(row.get("_rankingScore", 0.0) or 0.0)
        return hits

    async def federated_search(
        self,
        *,
        query: str,
        limit: int,
        filter_url: str | None,
        filter_username: str | None,
        prefix: bool,
        typo_tolerance: bool,
    ) -> tuple[list[dict[str, Any]], float]:
        started = asyncio.get_running_loop().time()
        tasks = [
            self.search_shard(
                shard_id=shard_id,
                query=query,
                limit=limit,
                offset=0,
                filter_url=filter_url,
                filter_username=filter_username,
                prefix=prefix,
                typo_tolerance=typo_tolerance,
            )
            for shard_id in range(self.shard_count)
        ]
        responses = await asyncio.gather(*tasks, return_exceptions=True)

        merged: list[dict[str, Any]] = []
        for result in responses:
            if isinstance(result, Exception):
                continue
            merged.extend(result)

        merged.sort(key=lambda item: item.get("score", 0.0), reverse=True)
        took_ms = (asyncio.get_running_loop().time() - started) * 1000
        return merged[:limit], took_ms


meili_router = MeiliShardRouter()
