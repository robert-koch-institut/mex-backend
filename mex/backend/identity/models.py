from mex.common.models import BaseModel
from mex.common.types import MergedPrimarySourceIdentifier


class IdentityAssignRequest(BaseModel):
    """Request body for identity upsert requests."""

    hadPrimarySource: MergedPrimarySourceIdentifier
    identifierInPrimarySource: str
