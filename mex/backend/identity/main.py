from fastapi import APIRouter
from fastapi.responses import JSONResponse

from mex.backend.graph.connector import GraphConnector
from mex.backend.graph.transform import transform_identity_result_to_identity
from mex.backend.identity.models import IdentityAssignRequest, IdentityFetchResponse
from mex.backend.transform import to_primitive
from mex.common.exceptions import MExError
from mex.common.identity.models import Identity
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
        identity = transform_identity_result_to_identity(graph_result.data[0])
    else:
        identity = Identity(
            hadPrimarySource=request.hadPrimarySource,
            identifier=Identifier.generate(),
            identifierInPrimarySource=request.identifierInPrimarySource,
            stableTargetId=Identifier.generate(),
        )
    return JSONResponse(to_primitive(identity), 200)  # type: ignore[return-value]


@router.get("/identity", status_code=200, tags=["extractors"])
def fetch_identity(
    hadPrimarySource: Identifier | None = None,  # noqa: N803
    identifierInPrimarySource: str | None = None,  # noqa: N803
    stableTargetId: Identifier | None = None,  # noqa: N803
) -> IdentityFetchResponse:
    """Find an Identity instance from the database if it can be found."""
    connector = GraphConnector.get()
    graph_result = connector.fetch_identities(
        had_primary_source=hadPrimarySource,
        identifier_in_primary_source=identifierInPrimarySource,
        stable_target_id=stableTargetId,
    )
    identities = [
        transform_identity_result_to_identity(result) for result in graph_result.data
    ]
    response = IdentityFetchResponse(items=identities, total=len(identities))
    return JSONResponse(to_primitive(response), 200)  # type: ignore[return-value]
