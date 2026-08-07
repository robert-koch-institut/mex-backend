from typing import Annotated, Final

from fastapi import APIRouter, Body, Depends, Request
from pydantic import Field
from starlette import status
from starlette.concurrency import run_in_threadpool

from mex.backend.graph.connector import GraphConnector
from mex.backend.security import has_write_access
from mex.common.logging import logger
from mex.common.models import AnyExtractedModel, AnyRuleSetResponse

router = APIRouter()

EXHAUSTED: Final = object()


@router.post(
    "/ingest",
    status_code=status.HTTP_204_NO_CONTENT,
    tags=["extractors"],
    dependencies=[Depends(has_write_access)],
)
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
    """Ingest a batch of extracted or rule-set items."""
    connector = GraphConnector.get()
    generator = connector.ingest_items(items)
    index = 0
    try:
        # the generator does blocking neo4j i/o, so advance it in a worker thread
        # to keep the event loop free for other requests during long ingests.
        # we use the sentinel object `EXHAUSTED` here because we can't intercept
        # see: https://docs.python.org/3/library/exceptions.html#StopIteration
        while await run_in_threadpool(next, generator, EXHAUSTED) is not EXHAUSTED:
            index += 1
            if await request.is_disconnected():
                logger.warning(f"client disconnected after {index} items were ingested")
                break
    finally:
        # when the generator does not exhaust in time, either because the client
        # disconnected or because uvicorn is shutting down and raised a CancelledError,
        # we need to manually close the generator, to release the neo4j session.
        await run_in_threadpool(generator.close)
