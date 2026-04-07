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
from mex.backend.main import lifespan
from mex.backend.routers import public_router, read_router, write_router
from mex.backend.settings import BackendSettings
from mex.backend.testing.routers import database_deletion_router, mocked_ldap_router
from mex.common.cli import entrypoint

app = FastAPI(
    title="mex-backend-testing",
    summary="Robert Koch-Institut Metadata Exchange testing API",
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
router.include_router(database_deletion_router)
router.include_router(mocked_ldap_router)

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
    """Start the testing backend server process.

    Initializes and runs the FastAPI application using uvicorn server.
    Loads configuration from BackendSettings and starts the HTTP server
    on the configured host and port.
    """
    settings = BackendSettings.get()
    settings.debug = True
    uvicorn.run(
        "mex.backend.testing.main:app",
        host=settings.backend_host,
        port=settings.backend_port,
        root_path=settings.backend_root_path,
        reload=settings.debug,
        log_config=UVICORN_LOGGING_CONFIG,
        headers=[("server", "mex-backend-testing")],
    )
