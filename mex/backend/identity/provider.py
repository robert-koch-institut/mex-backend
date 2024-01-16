from functools import cache

from mex.backend.graph.connector import GraphConnector
from mex.backend.graph.transform import transform_identity_result_to_identity
from mex.common.exceptions import MExError
from mex.common.identity import BaseProvider, Identity
from mex.common.models import (
    MEX_PRIMARY_SOURCE_IDENTIFIER_IN_PRIMARY_SOURCE,
    MEX_PRIMARY_SOURCE_STABLE_TARGET_ID,
)
from mex.common.types import Identifier, PrimarySourceID


class GraphIdentityProvider(BaseProvider, GraphConnector):
    """Identity provider that communicates with the neo4j graph database."""

    @cache
    def assign(
        self,
        had_primary_source: PrimarySourceID,
        identifier_in_primary_source: str,
    ) -> Identity:
        """Find an Identity in the database or assign a new one."""
        graph_result = self.fetch_identities(
            had_primary_source=had_primary_source,
            identifier_in_primary_source=identifier_in_primary_source,
        )
        if len(graph_result.data) > 1:
            raise MExError("found multiple identities indicating graph inconsistency")
        if len(graph_result.data) == 1:
            return transform_identity_result_to_identity(graph_result.data[0])
        if (
            identifier_in_primary_source
            == MEX_PRIMARY_SOURCE_IDENTIFIER_IN_PRIMARY_SOURCE
            and had_primary_source == MEX_PRIMARY_SOURCE_STABLE_TARGET_ID
        ):
            # This is to deal with the edge case where primary source is the parent of
            # all primary sources and has no parents for itself,
            # this will add itself as its parent.
            return Identity(
                hadPrimarySource=had_primary_source,
                identifier=MEX_PRIMARY_SOURCE_STABLE_TARGET_ID,
                identifierInPrimarySource=identifier_in_primary_source,
                stableTargetId=MEX_PRIMARY_SOURCE_STABLE_TARGET_ID,
            )
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
        graph_result = self.fetch_identities(
            had_primary_source=had_primary_source,
            identifier_in_primary_source=identifier_in_primary_source,
            stable_target_id=stable_target_id,
        )
        return [
            transform_identity_result_to_identity(result)
            for result in graph_result.data
        ]
