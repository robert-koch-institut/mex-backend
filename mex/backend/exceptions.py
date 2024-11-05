from typing import Any, cast

from fastapi.encoders import jsonable_encoder
from pydantic import BaseModel, ValidationError
from starlette import status
from starlette.requests import Request
from starlette.responses import Response

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


def handle_validation_error(request: Request, exc: Exception) -> Response:
    """Handle pydantic validation errors and provide debugging info."""
    logger.exception("ValidationError %s", exc)
    return Response(
        content=ErrorResponse(
            message=str(exc),
            debug=DebuggingInfo(
                errors=[
                    jsonable_encoder(e) for e in cast(ValidationError, exc).errors()
                ],
                scope=DebuggingScope.model_validate(request.scope),
            ),
        ).model_dump_json(),
        status_code=status.HTTP_400_BAD_REQUEST,
        media_type="application/json",
    )


def handle_uncaught_exception(request: Request, exc: Exception) -> Response:
    """Handle uncaught errors and provide debugging info."""
    logger.exception("UncaughtError %s", exc)
    return Response(
        content=ErrorResponse(
            message=str(exc),
            debug=DebuggingInfo(
                errors=[{"type": type(exc).__name__}],
                scope=DebuggingScope.model_validate(request.scope),
            ),
        ).model_dump_json(),
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        media_type="application/json",
    )
