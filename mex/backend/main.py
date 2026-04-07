from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any

import uvicorn
from fastapi import APIRouter, FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic_core import SchemaError, ValidationError

from mex.backend.exceptions import (
    BackendError,
    handle_detailed_error,
    handle_uncaught_exception,
)
from mex.backend.logging import UVICORN_LOGGING_CONFIG
from mex.backend.routers import (
    ldap_login_router,
    public_router,
    read_router,
    read_router_talking_to_ldap,
    write_router,
)
from mex.backend.settings import BackendSettings
from mex.common.cli import entrypoint
from mex.common.connector import CONNECTOR_STORE
from mex.common.logging import logger

if TYPE_CHECKING:  # pragma: no cover
    from collections.abc import AsyncIterator, Callable

startup_tasks: list[Callable[[], Any]] = [
    BackendSettings.get,
]
teardown_tasks: list[Callable[[], Any]] = [
    CONNECTOR_STORE.reset,
]


@asynccontextmanager
async def lifespan(_: FastAPI) -> AsyncIterator[None]:
    """Async context manager to execute startup and teardown of the FastAPI app."""
    for task in startup_tasks:
        task()
        task_name = getattr(task, "__wrapped__", task).__name__
        logger.info(f"startup {task_name} complete")
    yield None
    for task in teardown_tasks:
        task()
        task_name = getattr(task, "__wrapped__", task).__name__
        logger.info(f"teardown {task_name} complete")


app = FastAPI(
    title="mex-backend",
    summary="Robert Koch-Institut Metadata Exchange API",
    description=(
        "The MEx API includes endpoints for multiple use-cases, "
        "e.g. for extractor pipelines, the MEx editor or inter-departmental access."
    ),
    contact={
        "name": "RKI MEx Team",
        "email": "mex@rki.de",
        "url": "https://github.com/robert-koch-institut/mex-backend",
    },
    strict_content_type=False,
    lifespan=lifespan,
    version="v0",
)
router = APIRouter(prefix="/v0")
router.include_router(read_router)
router.include_router(write_router)
router.include_router(public_router)
router.include_router(ldap_login_router)
router.include_router(read_router_talking_to_ldap)

app.include_router(router)
app.add_exception_handler(BackendError, handle_detailed_error)
app.add_exception_handler(SchemaError, handle_detailed_error)
app.add_exception_handler(ValidationError, handle_detailed_error)
app.add_exception_handler(Exception, handle_uncaught_exception)
app.add_middleware(
    CORSMiddleware,
    allow_credentials=True,
    allow_headers=["*"],
    allow_methods=["*"],
    allow_origins=["*"],
)


@entrypoint(BackendSettings)
def main() -> None:  # pragma: no cover
    """Start the backend server process.

    Initializes and runs the FastAPI application using uvicorn server.
    Loads configuration from BackendSettings and starts the HTTP server
    on the configured host and port.
    """
    settings = BackendSettings.get()
    uvicorn.run(
        "mex.backend.main:app",
        host=settings.backend_host,
        port=settings.backend_port,
        root_path=settings.backend_root_path,
        reload=settings.debug,
        log_config=UVICORN_LOGGING_CONFIG,
        headers=[("server", "mex-backend")],
    )
