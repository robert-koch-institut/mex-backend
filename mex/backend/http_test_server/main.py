import mimetypes
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any

import uvicorn
from fastapi import APIRouter, FastAPI, HTTPException
from fastapi.responses import FileResponse, RedirectResponse, Response

from mex.backend.exceptions import (
    BackendError,
)
from mex.backend.http_test_server.settings import HttpTestServerSettings
from mex.backend.logging import UVICORN_LOGGING_CONFIG
from mex.common.cli import entrypoint
from mex.common.connector import CONNECTOR_STORE
from mex.common.logging import logger

if TYPE_CHECKING:  # pragma: no cover
    from collections.abc import AsyncIterator, Callable
startup_tasks: list[Callable[[], Any]] = [
    HttpTestServerSettings.get,
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
    title="mex-http-test-server",
    summary="Robert Koch-Institut Metadata Exchange http test server API",
    description=(
        "The MEx http test server API includes endpoints for multiple test-cases, "
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


@router.post("/http-test-server/datscha_web/login.php")
def post_datscha_web_login() -> RedirectResponse:
    """Login logic for datscha web."""
    return RedirectResponse("verzeichnis.php")


@router.api_route(
    "/http-test-server/{test_data_path:path}", methods=["GET", "POST"]
)
def http_test_server(test_data_path: str) -> FileResponse:
    """Return http server test data defined in mex-assets."""
    settings = HttpTestServerSettings.get()
    path_to_file_without_ext = (
        settings.http_test_server_test_data_directory / test_data_path
    )

    found_files = list(
        path_to_file_without_ext.parent.glob(path_to_file_without_ext.name + ".*")
    )

    len_found_files = len(found_files)
    if len_found_files == 0:
        msg= f"No files found for '/http-test-server/{test_data_path}'" 
        raise HTTPException(status_code=404, detail=msg)
    if len_found_files > 1:
        msg = f"Too many files found for '/http-test-server/{test_data_path}'"
        raise HTTPException(status_code=404, detail=msg)

    found_file = found_files[0]
    mimetype, _ = mimetypes.guess_file_type(found_file)

    return FileResponse(found_file, media_type=mimetype)


@router.head("/http-test-server/{_:path}")
def head_http_test_server(_: str) -> Response:
    """HEAD endpoint for checking availibility."""
    return Response(status_code=200)


app.include_router(router)


@entrypoint(HttpTestServerSettings)
def main() -> None:  # pragma: no cover
    """Start the http test server process.

    Initializes and runs the FastAPI application using uvicorn server.
    Loads configuration from HttpTestServerSettings and starts the HTTP test server
    on the configured host and port.
    """

    settings = HttpTestServerSettings.get()
    uvicorn.run(
        "mex.backend.http_test_server.main:app",
        host=settings.http_test_server_host,
        port=settings.http_test_server_port,
        root_path=settings.http_test_server_root_path,
        reload=settings.debug,
        log_config=UVICORN_LOGGING_CONFIG,
        headers=[("server", "mex-http-test-server")],
    )
