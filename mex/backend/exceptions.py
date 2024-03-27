from typing import Any, cast

from fastapi.responses import JSONResponse
from pydantic import BaseModel, ValidationError
from starlette.requests import Request

from mex.backend.transform import to_primitive
from mex.common.logging import logger


class DebuggingScope(BaseModel, extra="ignore"):
    """Scope for debugging info of error responses."""

    http_version: str
    method: str
    path: str
    path_params: dict[str, Any]
    query_string: str
    scheme: str


class DebuggingInfo(BaseModel):
    """Debugging information for error responses."""

    errors: list[dict[str, Any]]
    scope: DebuggingScope


class ErrorResponse(BaseModel):
    """Response model for user and system errors."""

    message: str
    debug: DebuggingInfo


def handle_validation_error(request: Request, exc: Exception) -> JSONResponse:
    """Handle uncaught errors and provide debugging info."""
    logger.exception("Error %s", exc)
    return JSONResponse(
        to_primitive(
            ErrorResponse(
                message=str(exc),
                debug=DebuggingInfo(
                    errors=cast(ValidationError, exc).errors(),
                    scope=DebuggingScope(**request.scope),
                ),
            )
        ),
        400,
    )


def handle_uncaught_exception(request: Request, exc: Exception) -> JSONResponse:
    """Handle uncaught errors and provide debugging info."""
    logger.exception("Error %s", exc)
    return JSONResponse(
        to_primitive(
            ErrorResponse(
                message=str(exc),
                debug=DebuggingInfo(
                    errors=[dict(type=type(exc).__name__)],
                    scope=DebuggingScope(**request.scope),
                ),
            )
        ),
        500,
    )
