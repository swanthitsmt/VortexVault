from __future__ import annotations

from redis import Redis

from fluxdb.config import settings


class DedupeService:
    def __init__(self) -> None:
        self._redis = Redis.from_url(settings.redis_url, decode_responses=True)
        self.bloom_key = "fluxdb:v2:bloom"
        self.hll_key = "fluxdb:v2:hll"
        self.fallback_set_key = "fluxdb:v2:fallback_set"

    def ensure_filter(self) -> None:
        try:
            # Capacity is intentionally very high for large datasets.
            self._redis.execute_command("BF.RESERVE", self.bloom_key, 0.0001, 2_000_000_000)
        except Exception:
            pass

    def is_new(self, digest_hex: str) -> bool:
        try:
            added = int(self._redis.execute_command("BF.ADD", self.bloom_key, digest_hex))
            self._redis.pfadd(self.hll_key, digest_hex)
            return added == 1
        except Exception:
            added = int(self._redis.sadd(self.fallback_set_key, digest_hex))
            self._redis.pfadd(self.hll_key, digest_hex)
            return added == 1

    def cardinality_estimate(self) -> int:
        return int(self._redis.pfcount(self.hll_key) or 0)


dedupe_service = DedupeService()
