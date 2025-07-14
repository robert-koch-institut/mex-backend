from typing import Annotated

from fastapi import APIRouter, Body, HTTPException, status

from mex.backend.match.helper import match_item_in_graph
from mex.common.exceptions import MExError
from mex.common.types import AnyExtractedIdentifier, AnyMergedIdentifier

router = APIRouter()


@router.post("/", status_code=status.HTTP_204_NO_CONTENT)
def match_item(
    identifier: Annotated[AnyExtractedIdentifier, Body()],
    stableTargetId: Annotated[AnyMergedIdentifier, Body()],
) -> None:
    """Match an extracted item to a merged item."""
    try:
        match_item_in_graph(identifier, stableTargetId)
    except MExError as error:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=list(error.args),
        ) from None
