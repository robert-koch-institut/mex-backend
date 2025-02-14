from fastapi import APIRouter
from starlette import status

from mex.backend.graph.connector import GraphConnector
from mex.common.models import (
    AnyExtractedModel,
    AnyRuleSetResponse,
    ItemsContainer,
)

router = APIRouter()


@router.post("/ingest", status_code=status.HTTP_201_CREATED, tags=["extractors"])
def ingest(
    request: ItemsContainer[AnyExtractedModel | AnyRuleSetResponse],
) -> ItemsContainer[AnyExtractedModel | AnyRuleSetResponse]:
    """Ingest a batch of extracted items or rule-sets and return them."""
    connector = GraphConnector.get()
    return ItemsContainer[AnyExtractedModel | AnyRuleSetResponse](
        items=connector.ingest(request.items)
    )
