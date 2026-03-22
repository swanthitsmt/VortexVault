from __future__ import annotations

import logging
from collections.abc import AsyncGenerator
from typing import Any

from elasticsearch import AsyncElasticsearch
from elasticsearch.helpers import async_bulk

from app.config import settings

logger = logging.getLogger(__name__)

INDEX_NAME = "combo_entries"

INDEX_SETTINGS: dict[str, Any] = {
    "settings": {
        "number_of_shards": settings.es_number_of_shards,
        "number_of_replicas": settings.es_number_of_replicas,
        "refresh_interval": "5s",
        "index.max_result_window": 100_000,
    },
    "mappings": {
        "properties": {
            "id": {"type": "long"},
            "url": {"type": "text", "analyzer": "standard", "fields": {"raw": {"type": "keyword"}}},
            "username": {"type": "text", "analyzer": "standard", "fields": {"raw": {"type": "keyword"}}},
            "password": {"type": "text", "analyzer": "standard", "fields": {"raw": {"type": "keyword"}}},
            "digest": {"type": "keyword"},
            "created_at": {"type": "date"},
        }
    },
}

_client: AsyncElasticsearch | None = None


def get_es_client() -> AsyncElasticsearch:
    """Get or create the singleton async ES client."""
    global _client
    if _client is None:
        _client = AsyncElasticsearch(
            settings.es_url,
            request_timeout=settings.es_timeout,
            max_retries=2,
            retry_on_timeout=True,
        )
    return _client


async def close_es_client() -> None:
    global _client
    if _client is not None:
        await _client.close()
        _client = None


async def ensure_index() -> None:
    """Create or upgrade the combo_entries index for current search schema."""
    client = get_es_client()
    try:
        exists = await client.indices.exists(index=INDEX_NAME)
        if not exists:
            await client.indices.create(index=INDEX_NAME, body=INDEX_SETTINGS)
            logger.info("Created Elasticsearch index: %s", INDEX_NAME)
            return

        # Forward-compatible mapping update for existing indices.
        await client.indices.put_mapping(
            index=INDEX_NAME,
            properties=INDEX_SETTINGS["mappings"]["properties"],
        )

        # Backfill numeric `id` for documents indexed before this field existed.
        await client.update_by_query(
            index=INDEX_NAME,
            body={
                "query": {"bool": {"must_not": {"exists": {"field": "id"}}}},
                "script": {
                    "source": "ctx._source.id = Long.parseLong(ctx._id)",
                    "lang": "painless",
                },
            },
            conflicts="proceed",
            refresh=True,
            wait_for_completion=False,
        )
    except Exception:
        logger.exception("Failed to create Elasticsearch index")


async def es_health_check() -> bool:
    """Check if Elasticsearch is reachable."""
    try:
        client = get_es_client()
        info = await client.info()
        return bool(info)
    except Exception:
        return False


async def es_index_doc_count() -> int:
    """Return current document count in combo_entries index."""
    try:
        client = get_es_client()
        result = await client.count(index=INDEX_NAME, body={"query": {"match_all": {}}})
        return int(result.get("count", 0))
    except Exception:
        logger.exception("Failed to fetch Elasticsearch document count")
        return 0


# ---------------------------------------------------------------------------
# Bulk sync from PostgreSQL → Elasticsearch
# ---------------------------------------------------------------------------

def _actions_from_rows(rows: list[tuple]) -> AsyncGenerator[dict, None]:
    """Generate ES bulk action dicts from DB rows (id, url, username, password, created_at)."""
    async def _gen():
        for row in rows:
            yield {
                "_index": INDEX_NAME,
                "_id": str(row[0]),
                "_source": {
                    "id": int(row[0]),
                    "url": row[1],
                    "username": row[2],
                    "password": row[3],
                    "created_at": row[4].isoformat() if row[4] else None,
                },
            }
    return _gen()


async def bulk_index_rows(rows: list[tuple]) -> int:
    """Bulk index rows into Elasticsearch. Returns count of indexed docs."""
    if not rows:
        return 0
    client = get_es_client()
    success, errors = await async_bulk(
        client,
        _actions_from_rows(rows),
        chunk_size=2000,
        raise_on_error=False,
    )
    if errors:
        logger.warning("ES bulk index had %d error(s)", len(errors))
    return success


# ---------------------------------------------------------------------------
# Search via Elasticsearch
# ---------------------------------------------------------------------------

async def es_search(
    *,
    url: str | None,
    username: str | None,
    password: str | None,
    regex: bool,
    page_size: int,
    cursor: int | None,
    direction: str,
    include_total: bool,
) -> dict:
    """Execute search against Elasticsearch with cursor-based pagination."""
    import time

    client = get_es_client()
    start = time.perf_counter()

    must_clauses: list[dict] = []

    def _escape_wildcard_term(value: str) -> str:
        # wildcard query treats `*` and `?` specially; escape them for literal contains search.
        return value.replace("\\", "\\\\").replace("*", "\\*").replace("?", "\\?")

    for field, value in [("url", url), ("username", username), ("password", password)]:
        if value:
            if regex:
                must_clauses.append({"regexp": {field: {"value": value, "flags": "ALL", "case_insensitive": True}}})
            else:
                must_clauses.append(
                    {
                        "wildcard": {
                            f"{field}.raw": {
                                "value": f"*{_escape_wildcard_term(value)}*",
                                "case_insensitive": True,
                            }
                        }
                    }
                )

    query: dict[str, Any] = {"bool": {"must": must_clauses}} if must_clauses else {"match_all": {}}

    body: dict[str, Any] = {
        "query": query,
        "size": page_size + 1,
        "sort": [{"id": {"order": "desc"}}],
        "_source": ["id", "url", "username", "password"],
    }

    if cursor is not None:
        if direction == "prev":
            body["sort"] = [{"id": {"order": "asc"}}]
            body["query"] = {
                "bool": {
                    "must": must_clauses,
                    "filter": [{"range": {"id": {"gt": cursor}}}],
                }
            }
        else:
            body["query"] = {
                "bool": {
                    "must": must_clauses,
                    "filter": [{"range": {"id": {"lt": cursor}}}],
                }
            }

    result = await client.search(index=INDEX_NAME, body=body)

    hits = result["hits"]["hits"]

    total: int | None = None
    if include_total:
        count_body: dict[str, Any] = {"query": query}
        count_result = await client.count(index=INDEX_NAME, body=count_body)
        total = int(count_result["count"])

    if direction == "prev":
        has_prev = len(hits) > page_size
        page_hits = hits[:page_size]
        page_hits.reverse()
        has_next = cursor is not None
    else:
        has_next = len(hits) > page_size
        page_hits = hits[:page_size]
        has_prev = cursor is not None

    rows = []
    for hit in page_hits:
        src = hit["_source"]
        row_id = src.get("id")
        if row_id is None:
            row_id = int(hit["_id"])
        rows.append(type("Row", (), {
            "id": int(row_id),
            "url": src.get("url", ""),
            "username": src.get("username", ""),
            "password": src.get("password", ""),
        })())

    first_id = int(rows[0].id) if rows else None
    last_id = int(rows[-1].id) if rows else None
    next_cursor = last_id if has_next and last_id is not None else None
    prev_cursor = first_id if has_prev and first_id is not None else None

    elapsed_ms = (time.perf_counter() - start) * 1000

    return {
        "total": total,
        "page_size": page_size,
        "execution_ms": elapsed_ms,
        "rows": rows,
        "cursor": cursor,
        "direction": direction,
        "has_next": has_next,
        "has_prev": has_prev,
        "next_cursor": next_cursor,
        "prev_cursor": prev_cursor,
        "first_id": first_id,
        "last_id": last_id,
    }
