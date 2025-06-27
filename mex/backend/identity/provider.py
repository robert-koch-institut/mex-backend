from mex.backend.cache.connector import CacheConnector
from mex.backend.graph.connector import GraphConnector
from mex.common.identity import BaseProvider, Identity
from mex.common.types import Identifier, MergedPrimarySourceIdentifier


class GraphIdentityProvider(BaseProvider):
    """Identity provider that communicates with the graph connector."""

    def __init__(self) -> None:
        """Create a new graph identity provider."""
        self._cache = CacheConnector.get()
        self._identities_generated = 0

    def assign(
        self,
        had_primary_source: MergedPrimarySourceIdentifier,
        identifier_in_primary_source: str,
    ) -> Identity:
        """Return a cached or a newly assigned Identity.

        Retrieves an existing identity from cache or database, or creates a new one
        if no matching identity exists. Caches the result for future lookups.

        Args:
            had_primary_source: The identifier of the primary source the item belongs to
            identifier_in_primary_source: The identifier within the primary source

        Returns:
            Identity object with provenance metadata
        """
        # newline is a safe delimiter because it is explicitly forbidden in both fields
        cache_key = f"{had_primary_source}\n{identifier_in_primary_source}"
        if cache_value := self._cache.get_value(cache_key):
            return Identity.model_validate(cache_value)
        connector = GraphConnector.get()
        result = connector.fetch_identities(
            had_primary_source=had_primary_source,
            identifier_in_primary_source=identifier_in_primary_source,
        )
        if graph_record := result.one_or_none():
            identity = Identity.model_validate(graph_record)
        else:
            identity = Identity(
                hadPrimarySource=had_primary_source,
                identifier=Identifier.generate(),
                identifierInPrimarySource=identifier_in_primary_source,
                stableTargetId=Identifier.generate(),
            )
            self._identities_generated += 1
        self._cache.set_value(cache_key, identity)
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
        return {"identities_generated": self._identities_generated}

    def close(self) -> None:
        """Close the connector cache."""
        self._cache.close()
