from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from itertools import chain
from typing import Any

import uvicorn
from fastapi import APIRouter, Depends, FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.openapi.utils import get_openapi
from pydantic import BaseModel, ValidationError

from mex.backend.auxiliary.wikidata import router as wikidata_router
from mex.backend.exceptions import handle_uncaught_exception, handle_validation_error
from mex.backend.extracted.main import router as extracted_router
from mex.backend.identity.main import router as identity_router
from mex.backend.ingest.main import router as ingest_router
from mex.backend.logging import UVICORN_LOGGING_CONFIG
from mex.backend.merged.main import router as merged_router
from mex.backend.preview.main import router as preview_router
from mex.backend.rules.main import router as rules_router
from mex.backend.security import has_read_access, has_write_access
from mex.backend.settings import BackendSettings
from mex.common.cli import entrypoint
from mex.common.connector import CONNECTOR_STORE
from mex.common.types import (
    EXTRACTED_IDENTIFIER_CLASSES,
    IDENTIFIER_PATTERN,
    MERGED_IDENTIFIER_CLASSES,
)


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
    for identifier in chain(EXTRACTED_IDENTIFIER_CLASSES, MERGED_IDENTIFIER_CLASSES):
        name = identifier.__name__
        openapi_schema["components"]["schemas"][name] = {
            "title": name,
            "type": "string",
            "description": identifier.__doc__,
            "pattern": IDENTIFIER_PATTERN,
        }

    app.openapi_schema = openapi_schema
    return app.openapi_schema


@asynccontextmanager
async def lifespan(_: FastAPI) -> AsyncIterator[None]:
    """Async context manager to execute setup and teardown of the FastAPI app."""
    yield None
    CONNECTOR_STORE.reset()


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
router.include_router(preview_router, dependencies=[Depends(has_read_access)])
router.include_router(rules_router, dependencies=[Depends(has_write_access)])
router.include_router(wikidata_router, dependencies=[Depends(has_read_access)])


class SystemStatus(BaseModel):
    """Response model for system status check."""

    status: str


@router.get("/_system/check", tags=["system"])
def check_system_status() -> SystemStatus:
    """Check that the backend server is healthy and responsive."""
    return SystemStatus(status="ok")


app.include_router(router)
app.add_exception_handler(ValidationError, handle_validation_error)
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
