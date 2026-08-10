import threading
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
    # the generator is driven from worker threads, so guard it with a lock: when the
    # request task is cancelled, anyio abandons the thread that is currently running
    # `next`, and closing a generator that is still executing raises a `ValueError`.
    lock = threading.Lock()
    index = 0

    def advance() -> object:
        with lock:
            return next(generator, EXHAUSTED)

    def close() -> None:
        with lock:
            generator.close()

    try:
        # the generator does blocking neo4j i/o, so advance it in a worker thread
        # to keep the event loop free for other requests during long ingests.
        # we use the sentinel object `EXHAUSTED` here because we can't intercept
        # see: https://docs.python.org/3/library/exceptions.html#StopIteration
        while await run_in_threadpool(advance) is not EXHAUSTED:
            index += 1
            if await request.is_disconnected():
                logger.warning(f"client disconnected after {index} items were ingested")
                break
    finally:
        # when the generator does not exhaust in time, either because the client
        # disconnected or because uvicorn is shutting down and raised a CancelledError,
        # we need to manually close the generator, to release the neo4j session.
        try:
            await run_in_threadpool(close)
        except BaseException:
            # uvicorn cancels in-flight request tasks on shutdown, which cancels the
            # await above before the worker gets to run `close`. a plain thread is the
            # one place the cancellation cannot reach, so finish the close there.
            threading.Thread(target=close, name="ingest-close").start()
            raise
