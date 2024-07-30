from typing import Annotated

from pydantic import Field

from mex.common.models import AnyExtractedModel, BaseModel


class ExtractedItemSearchResponse(BaseModel):
    """Response body for the extracted item search endpoint."""

    items: Annotated[list[AnyExtractedModel], Field(discriminator="entityType")]
    total: int
