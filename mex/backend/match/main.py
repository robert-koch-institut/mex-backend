from typing import Annotated

from fastapi import APIRouter, Body, HTTPException, status

from mex.backend.extracted.helpers import get_extracted_item_from_graph
from mex.backend.graph.exceptions import NoResultFoundError
from mex.backend.match.helper import match_item_in_graph
from mex.backend.merged.helpers import get_merged_item_from_graph
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
        extracted_item = get_extracted_item_from_graph(identifier)
    except NoResultFoundError:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Extracted item not found.",
        ) from None

    try:
        merged_item = get_merged_item_from_graph(stableTargetId)
    except NoResultFoundError:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Merged item not found.",
        ) from None

    try:
        match_item_in_graph(extracted_item, merged_item)
    except MExError as error:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=list(error.args),
        ) from None
