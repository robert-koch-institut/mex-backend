from typing import Any

from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ValidationError
from starlette import status
from starlette.requests import Request

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


def handle_uncaught_exception(request: Request, exc: Exception) -> JSONResponse:
    """Handle uncaught errors and provide debugging info."""
    logger.exception("Error %s", exc)
    if isinstance(exc, ValidationError):
        errors = [dict(error) for error in exc.errors()]
        status_code = status.HTTP_400_BAD_REQUEST
    else:
        errors = [dict(type=type(exc).__name__)]
        status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
    return JSONResponse(
        jsonable_encoder(
            ErrorResponse(
                message=str(exc),
                debug=DebuggingInfo(
                    errors=errors, scope=DebuggingScope.model_validate(request.scope)
                ),
            )
        ),
        status_code,
    )
