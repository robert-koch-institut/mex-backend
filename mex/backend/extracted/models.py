from pydantic import Field

from mex.common.models import AnyExtractedModel, BaseModel


class ExtractedItemSearchResponse(BaseModel):
    """Response body for the extracted item search endpoint."""

    total: int
    items: list[AnyExtractedModel] = Field(discriminator="entityType")
