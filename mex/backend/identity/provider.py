from functools import lru_cache

import redis

from mex.backend.graph.connector import GraphConnector
from mex.backend.settings import BackendSettings
from mex.common.identity import BaseProvider, Identity
from mex.common.types import Identifier, MergedPrimarySourceIdentifier

IDENTITY_CACHE_SIZE = 5000


class GraphIdentityProvider(BaseProvider):
    """Identity provider that communicates with the graph connector.

    It uses `functools.lru_cache` as an L1 cache and an optional redis as L2.
    """

    def __init__(self) -> None:
        """Create a new graph identity provider."""
        # mitigating https://docs.astral.sh/ruff/rules/cached-instance-method
        self._cached_assign = lru_cache(IDENTITY_CACHE_SIZE)(self._do_assign)
        redis_url = BackendSettings.get().redis_url
        self._redis = redis.from_url(redis_url) if redis_url else None

    def assign(
        self,
        had_primary_source: MergedPrimarySourceIdentifier,
        identifier_in_primary_source: str,
    ) -> Identity:
        """Return a cached or a newly assigned Identity."""
        return self._cached_assign(had_primary_source, identifier_in_primary_source)

    def _do_assign(
        self,
        had_primary_source: MergedPrimarySourceIdentifier,
        identifier_in_primary_source: str,
    ) -> Identity:
        """Find an Identity in the cache or graph or assign a new one."""
        cache_key = f"{had_primary_source}\n{identifier_in_primary_source}"
        if self._redis and (cache_value := self._redis.get(cache_key)):
            return Identity.model_validate_json(cache_value)
        connector = GraphConnector.get()
        result = connector.fetch_identities(
            had_primary_source=had_primary_source,
            identifier_in_primary_source=identifier_in_primary_source,
        )
        if graph_record := result.one_or_none():
            return Identity.model_validate(graph_record)
        identity = Identity(
            hadPrimarySource=had_primary_source,
            identifier=Identifier.generate(),
            identifierInPrimarySource=identifier_in_primary_source,
            stableTargetId=Identifier.generate(),
        )
        if self._redis:
            self._redis.set(cache_key, identity.model_dump_json())
        return identity

    def fetch(
        self,
        *,
        had_primary_source: Identifier | None = None,
        identifier_in_primary_source: str | None = None,
        stable_target_id: Identifier | None = None,
    ) -> list[Identity]:
        """Find Identity instances matching the given filters.

        Either provide `stable_target_id` or `had_primary_source`
        and `identifier_in_primary_source` together to get a unique result.
        """
        connector = GraphConnector.get()
        result = connector.fetch_identities(
            had_primary_source=had_primary_source,
            identifier_in_primary_source=identifier_in_primary_source,
            stable_target_id=stable_target_id,
        )
        return [Identity.model_validate(result) for result in result]

    def metrics(self) -> dict[str, int]:
        """Generate metrics about identity provider usage."""
        cache_info = self._cached_assign.cache_info()
        return {"cache_hits": cache_info.hits, "cache_misses": cache_info.misses}

    def close(self) -> None:
        """Clear the connector cache."""
        self._cached_assign.cache_clear()
