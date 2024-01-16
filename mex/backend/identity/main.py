from fastapi import APIRouter
from fastapi.exceptions import HTTPException

from mex.backend.identity.models import IdentityAssignRequest, IdentityFetchResponse
from mex.backend.identity.provider import GraphIdentityProvider
from mex.common.exceptions import MExError
from mex.common.identity.models import Identity
from mex.common.types import Identifier

router = APIRouter()


@router.post("/identity", status_code=200, tags=["extractors"])
def assign_identity(request: IdentityAssignRequest) -> Identity:
    """Insert a new identity or update an existing one."""
    identity_provider = GraphIdentityProvider.get()
    return identity_provider.assign(
        had_primary_source=request.hadPrimarySource,
        identifier_in_primary_source=request.identifierInPrimarySource,
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
    identity_provider = GraphIdentityProvider.get()
    try:
        identities = identity_provider.fetch(
            had_primary_source=hadPrimarySource,
            identifier_in_primary_source=identifierInPrimarySource,
            stable_target_id=stableTargetId,
        )
    except MExError as error:
        raise HTTPException(400, error.args)
    return IdentityFetchResponse(items=identities, total=len(identities))
