from fastapi import APIRouter
from starlette import status

from mex.backend.graph.connector import GraphConnector
from mex.common.models import AnyExtractedModel, AnyRuleSetResponse, ItemsContainer

router = APIRouter()


@router.post("/ingest", status_code=status.HTTP_204_NO_CONTENT, tags=["extractors"])
def ingest_items(
    request: ItemsContainer[AnyExtractedModel | AnyRuleSetResponse],
) -> None:
    """Ingest a batch of extracted items or rule-sets."""
    connector = GraphConnector.get()
    connector.ingest(request.items)
