from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

import uvicorn
from fastapi import APIRouter, Depends, FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.openapi.utils import get_openapi
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ValidationError
from starlette.requests import Request

from mex.backend.extracted.main import router as extracted_router
from mex.backend.identity.main import router as identity_router
from mex.backend.ingest.main import router as ingest_router
from mex.backend.logging import UVICORN_LOGGING_CONFIG
from mex.backend.merged.main import router as merged_router
from mex.backend.security import has_read_access, has_write_access
from mex.backend.settings import BackendSettings
from mex.backend.transform import to_primitive
from mex.common.cli import entrypoint
from mex.common.connector import CONNECTOR_STORE
from mex.common.exceptions import MExError
from mex.common.logging import logger
from mex.common.types import Identifier
from mex.common.types.identifier import MEX_ID_PATTERN


def create_openapi_schema() -> dict[str, Any]:
    """Create an OpenAPI schema for the backend.

    Settings:
        backend_api_url: MEx backend API url.

    Returns:
        OpenApi schema as dictionary
    """
    if app.openapi_schema:
        return app.openapi_schema

    settings = BackendSettings.get()
    openapi_schema = get_openapi(
        title=app.title,
        version=app.version,
        contact=app.contact,
        summary=app.summary,
        description=app.description,
        routes=app.routes,
        servers=[dict(url=settings.backend_api_url)],
    )
    for subclass in Identifier.__subclasses__():
        name = subclass.__name__
        openapi_schema["components"]["schemas"][name] = {
            "title": name,
            "type": "string",
            "description": subclass.__doc__,
            "pattern": MEX_ID_PATTERN,
        }

    app.openapi_schema = openapi_schema
    return app.openapi_schema


def close_connectors() -> None:
    """Try to close all connectors in the current context."""
    for connector in CONNECTOR_STORE:
        try:
            connector.close()
        except Exception:
            logger.exception("Error closing %s", type(connector))
        else:
            logger.info("Closed %s", type(connector))
    CONNECTOR_STORE.reset()


@asynccontextmanager
async def lifespan(_: FastAPI) -> AsyncIterator[None]:
    """Async context manager to execute setup and teardown of the FastAPI app."""
    yield None
    close_connectors()


app = FastAPI(
    title="mex-backend",
    summary="Robert Koch-Institut Metadata Exchange API",
    description=(
        "The MEx API includes endpoints for multiple use-cases, "
        "e.g. for extractor pipelines, the MEx editor or inter-departmental access."
    ),
    contact=dict(
        name="RKI MEx Team",
        email="mex@rki.de",
        url="https://github.com/robert-koch-institut/mex-backend",
    ),
    lifespan=lifespan,
    version="v0",
)
router = APIRouter(prefix="/v0")
app.openapi = create_openapi_schema  # type: ignore[method-assign]
router.include_router(extracted_router, dependencies=[Depends(has_read_access)])
router.include_router(identity_router, dependencies=[Depends(has_write_access)])
router.include_router(ingest_router, dependencies=[Depends(has_write_access)])
router.include_router(merged_router, dependencies=[Depends(has_read_access)])


class SystemStatus(BaseModel):
    """Response model for system status check."""

    status: str


@router.get("/_system/check", tags=["system"])
def check_system_status() -> SystemStatus:
    """Check that the backend server is healthy and responsive."""
    return SystemStatus(status="ok")


def handle_uncaught_exception(request: Request, exc: Exception) -> JSONResponse:
    """Handle uncaught errors and provide some debugging clues."""
    logger.exception("Error %s", exc)
    if isinstance(exc, ValidationError):
        errors: list[Any] = exc.errors()
    else:
        errors = [dict(type=type(exc).__name__)]
    body = dict(message=str(exc), debug=dict(errors=errors))
    return JSONResponse(to_primitive(body), 500)


app.include_router(router)
app.add_exception_handler(ValidationError, handle_uncaught_exception)
app.add_exception_handler(MExError, handle_uncaught_exception)
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
    """Start the backend server process."""
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
