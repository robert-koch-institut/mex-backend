from mex.common.models import (
    AnyExtractedModel,
    BaseModel,
)
from mex.common.types import Identifier


class BulkIngestRequest(BaseModel):
    """Request body for the bulk ingestion endpoint."""

    items: list[AnyExtractedModel]


class BulkIngestResponse(BaseModel):
    """Response body for the bulk ingestion endpoint."""

    identifiers: list[Identifier]
