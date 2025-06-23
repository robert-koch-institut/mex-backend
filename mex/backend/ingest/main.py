from typing import Annotated

from fastapi import APIRouter, Body, Request
from pydantic import Field
from starlette import status

from mex.backend.graph.connector import GraphConnector
from mex.common.logging import logger
from mex.common.models import AnyExtractedModel, AnyRuleSetResponse

router = APIRouter()


@router.post("/ingest", status_code=status.HTTP_204_NO_CONTENT, tags=["extractors"])
async def ingest_items(
    request: Request,
    items: Annotated[
        list[
            Annotated[
                AnyExtractedModel | AnyRuleSetResponse,
                Field(discriminator="entityType"),
            ]
        ],
        Body(embed=True),
    ],
) -> None:
    """Ingest a batch of extracted items or rule-sets."""
    connector = GraphConnector.get()
    for i, _ in enumerate(connector.ingest_v2(items), start=1):
        if await request.is_disconnected():
            logger.warning(f"client disconnected after {i} items were ingested")
            break
