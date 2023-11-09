from mex.common.models import BaseModel
from mex.common.types import PrimarySourceID


class IdentityAssignRequest(BaseModel):
    """Request body for identity upsert requests."""

    hadPrimarySource: PrimarySourceID
    identifierInPrimarySource: str
