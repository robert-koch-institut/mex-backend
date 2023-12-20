from fastapi import APIRouter
from fastapi.exceptions import HTTPException

from mex.backend.graph.connector import GraphConnector
from mex.backend.graph.transform import transform_identity_result_to_identity
from mex.backend.identity.models import IdentityAssignRequest, IdentityFetchResponse
from mex.common.exceptions import MExError
from mex.common.identity.models import Identity
from mex.common.models import (
    MEX_PRIMARY_SOURCE_IDENTIFIER_IN_PRIMARY_SOURCE,
    MEX_PRIMARY_SOURCE_STABLE_TARGET_ID,
)
from mex.common.types import Identifier

router = APIRouter()


@router.post("/identity", status_code=200, tags=["extractors"])
def assign_identity(request: IdentityAssignRequest) -> Identity:
    """Insert a new identity or update an existing one."""
    connector = GraphConnector.get()
    graph_result = connector.fetch_identities(
        had_primary_source=request.hadPrimarySource,
        identifier_in_primary_source=request.identifierInPrimarySource,
    )
    if len(graph_result.data) > 1:
        raise MExError("found multiple identities indicating graph inconsistency")
    if len(graph_result.data) == 1:
        return transform_identity_result_to_identity(graph_result.data[0])
    if (
        request.identifierInPrimarySource
        == MEX_PRIMARY_SOURCE_IDENTIFIER_IN_PRIMARY_SOURCE
        and request.hadPrimarySource == MEX_PRIMARY_SOURCE_STABLE_TARGET_ID
    ):
        # This is to deal with the edge case where primary source is the parent of
        # all primary sources and has no parents for itself,
        # this will add itself as its parent.
        return Identity(
            hadPrimarySource=request.hadPrimarySource,
            identifier=MEX_PRIMARY_SOURCE_STABLE_TARGET_ID,
            identifierInPrimarySource=request.identifierInPrimarySource,
            stableTargetId=MEX_PRIMARY_SOURCE_STABLE_TARGET_ID,
        )
    return Identity(
        hadPrimarySource=request.hadPrimarySource,
        identifier=Identifier.generate(),
        identifierInPrimarySource=request.identifierInPrimarySource,
        stableTargetId=Identifier.generate(),
    )


@router.get("/identity", status_code=200, tags=["extractors"])
def fetch_identity(
    hadPrimarySource: Identifier | None = None,  # noqa: N803
    identifierInPrimarySource: str | None = None,  # noqa: N803
    stableTargetId: Identifier | None = None,  # noqa: N803
) -> IdentityFetchResponse:
    """Find an Identity instance from the database if it can be found.

    Either provide `stableTargetId` or `hadPrimarySource`
    and `identifierInPrimarySource` together to get a unique result.
    """
    connector = GraphConnector.get()
    try:
        graph_result = connector.fetch_identities(
            had_primary_source=hadPrimarySource,
            identifier_in_primary_source=identifierInPrimarySource,
            stable_target_id=stableTargetId,
        )
    except MExError as error:
        raise HTTPException(400, error.args)
    identities = [
        transform_identity_result_to_identity(result) for result in graph_result.data
    ]
    return IdentityFetchResponse(items=identities, total=len(identities))
