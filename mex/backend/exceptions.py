from typing import Any, Protocol, cast

from fastapi.encoders import jsonable_encoder
from pydantic import BaseModel
from pydantic_core import ErrorDetails, SchemaError, ValidationError
from starlette import status
from starlette.requests import Request
from starlette.responses import Response

from mex.common.exceptions import MExError
from mex.common.logging import logger


class DetailedError(Protocol):
    """Protocol for errors that offer details."""

    def errors(self) -> list[ErrorDetails]:
        """Details about each underlying error."""


class BackendError(MExError):
    """Base backend error that offer details on underlying pydantic errors."""

    def errors(self) -> list[ErrorDetails]:
        """Details about underlying pydantic errors."""
        if isinstance(self.__cause__, SchemaError | ValidationError):
            return self.__cause__.errors()
        return []

    def is_retryable(self) -> bool:
        """Whether the error is retryable."""
        return False


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


def _handle_exception(
    exc: Exception, status_code: int, info: DebuggingInfo
) -> Response:
    error_response = ErrorResponse(message=str(exc).strip(" "), debug=info)
    response_body = error_response.model_dump_json(indent=4)
    logger.exception(
        "%s - %s - \n%s",
        status_code,
        type(exc).__name__,
        response_body,
    )
    return Response(
        content=response_body,
        status_code=status_code,
        media_type="application/json",
    )


def handle_detailed_error(request: Request, exc: Exception) -> Response:
    """Handle detailed errors and provide debugging info."""
    return _handle_exception(
        exc,
        status_code=(
            status.HTTP_429_TOO_MANY_REQUESTS
            if isinstance(exc, BackendError) and exc.is_retryable()
            else status.HTTP_400_BAD_REQUEST
        ),
        info=DebuggingInfo(
            errors=[jsonable_encoder(e) for e in cast("DetailedError", exc).errors()],
            scope=DebuggingScope.model_validate(request.scope),
        ),
    )


def handle_uncaught_exception(request: Request, exc: Exception) -> Response:
    """Handle uncaught errors and provide debugging info."""
    return _handle_exception(
        exc,
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        info=DebuggingInfo(
            errors=[{"type": type(exc).__name__}],
            scope=DebuggingScope.model_validate(request.scope),
        ),
    )
