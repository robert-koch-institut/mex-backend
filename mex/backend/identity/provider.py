from functools import lru_cache

from mex.backend.graph.connector import GraphConnector
from mex.common.identity import BaseProvider, Identity
from mex.common.types import Identifier, MergedPrimarySourceIdentifier


class GraphIdentityProvider(BaseProvider, GraphConnector):
    """Identity provider that communicates with the graph database."""

    def __init__(self) -> None:
        """Create a new graph identity provider."""
        super().__init__()
        # mitigating https://docs.astral.sh/ruff/rules/cached-instance-method
        self._cached_assign = lru_cache(5000)(self._do_assign)

    def assign(
        self,
        had_primary_source: MergedPrimarySourceIdentifier,
        identifier_in_primary_source: str,
    ) -> Identity:
        """Return a cached Identity from the database or newly assigned one."""
        return self._cached_assign(had_primary_source, identifier_in_primary_source)

    def _do_assign(
        self,
        had_primary_source: MergedPrimarySourceIdentifier,
        identifier_in_primary_source: str,
    ) -> Identity:
        """Find an Identity in the database or assign a new one."""
        result = self.fetch_identities(
            had_primary_source=had_primary_source,
            identifier_in_primary_source=identifier_in_primary_source,
        )
        if record := result.one_or_none():
            return Identity.model_validate(record)
        return Identity(
            hadPrimarySource=had_primary_source,
            identifier=Identifier.generate(),
            identifierInPrimarySource=identifier_in_primary_source,
            stableTargetId=Identifier.generate(),
        )

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
        result = self.fetch_identities(
            had_primary_source=had_primary_source,
            identifier_in_primary_source=identifier_in_primary_source,
            stable_target_id=stable_target_id,
        )
        return [Identity.model_validate(result) for result in result]
