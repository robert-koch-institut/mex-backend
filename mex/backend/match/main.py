from fastapi import APIRouter
from starlette import status

from mex.backend.match.helpers import match_items_in_graph
from mex.backend.match.models import MatchRequest

router = APIRouter()


@router.post("/match", status_code=status.HTTP_204_NO_CONTENT, tags=["editor"])
def match_items(request: MatchRequest) -> None:
    """Assign an extracted item to a new merged item."""
    match_items_in_graph(
        extracted_item_identifier=request.identifier,
        merged_item_identifier=request.stableTargetId,
    )
