from typing import Annotated

from pydantic import Field

from mex.common.models import BaseModel
from mex.common.types import AnyExtractedIdentifier, AnyMergedIdentifier


class MatchRequest(BaseModel):
    """Request body for matching two items."""

    identifier: Annotated[
        AnyExtractedIdentifier,
        Field(
            description=(
                "The identifier of an extracted item "
                "that is to be assigned to a new merged item."
            )
        ),
    ]
    stableTargetId: Annotated[
        AnyMergedIdentifier,
        Field(
            description=(
                "The identifier of a merged item "
                "that the extracted item is to be assigned to."
            )
        ),
    ]
