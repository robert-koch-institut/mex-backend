from typing import Annotated

from fastapi import Body, HTTPException, status

from mex.backend.match.helpers import match_item_in_graph
from mex.backend.routers import write_router
from mex.common.exceptions import MExError
from mex.common.types import AnyExtractedIdentifier, AnyMergedIdentifier


@write_router.post("/match", status_code=status.HTTP_204_NO_CONTENT)
def match_item(
    extractedIdentifier: Annotated[AnyExtractedIdentifier, Body()],
    mergedIdentifier: Annotated[AnyMergedIdentifier, Body()],
) -> None:
    """Match an extracted item to a merged item."""
    try:
        match_item_in_graph(extractedIdentifier, mergedIdentifier)
    except MExError as error:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=list(error.args),
        ) from None
