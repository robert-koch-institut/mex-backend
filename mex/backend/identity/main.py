from typing import Annotated

from fastapi import Body, Query

from mex.backend.identity.provider import GraphIdentityProvider
from mex.backend.routers import read_router, write_router
from mex.common.identity import Identity
from mex.common.models import PaginatedItemsContainer
from mex.common.types import Identifier, MergedPrimarySourceIdentifier


@write_router.post("/identity", tags=["extractors"])
def assign_identity(
    hadPrimarySource: Annotated[MergedPrimarySourceIdentifier, Body()],
    identifierInPrimarySource: Annotated[str, Body()],
) -> Identity:
    """Insert a new identity or update an existing one."""
    identity_provider = GraphIdentityProvider.get()
    return identity_provider.assign(
        had_primary_source=hadPrimarySource,
        identifier_in_primary_source=identifierInPrimarySource,
    )


@read_router.get("/identity", tags=["extractors"])
def fetch_identity(
    hadPrimarySource: Annotated[Identifier | None, Query()] = None,
    identifierInPrimarySource: Annotated[str | None, Query()] = None,
    stableTargetId: Annotated[Identifier | None, Query()] = None,
) -> PaginatedItemsContainer[Identity]:
    """Find an Identity instance from the database if it can be found.

    Either provide `stableTargetId` or a tuple of `hadPrimarySource`
    and `identifierInPrimarySource` to get a unique result.
    """
    identity_provider = GraphIdentityProvider.get()
    identities = identity_provider.fetch(
        had_primary_source=hadPrimarySource,
        identifier_in_primary_source=identifierInPrimarySource,
        stable_target_id=stableTargetId,
    )
    return PaginatedItemsContainer[Identity](items=identities, total=len(identities))
