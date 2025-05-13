from fastapi import APIRouter, Request
from starlette import status

from mex.backend.graph.connector import GraphConnector
from mex.common.logging import logger
from mex.common.models import AnyExtractedModel, AnyRuleSetResponse, ItemsContainer

router = APIRouter()


@router.post("/ingest", status_code=status.HTTP_204_NO_CONTENT, tags=["extractors"])
async def ingest_items(
    request: Request,
    container: ItemsContainer[AnyExtractedModel | AnyRuleSetResponse],
) -> None:
    """Ingest a batch of extracted items or rule-sets."""
    connector = GraphConnector.get()
    for i, _ in enumerate(connector.ingest_v2(container.items), start=1):
        if await request.is_disconnected():
            logger.warning(f"client disconnected after {i} items were ingested")
            break
