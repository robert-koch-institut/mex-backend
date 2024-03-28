from mex.common.identity.models import Identity
from mex.common.models import BaseModel
from mex.common.types import MergedPrimarySourceIdentifier


class IdentityAssignRequest(BaseModel):
    """Request body for identity upsert requests."""

    hadPrimarySource: MergedPrimarySourceIdentifier
    identifierInPrimarySource: str


class IdentityFetchResponse(BaseModel):
    """Response body for identity fetch requests."""

    items: list[Identity] = []
    total: int = 0
