from collections.abc import Sequence
from typing import Any

from pydantic_core import ErrorDetails

from mex.backend.exceptions import BackendError


class NoResultFoundError(BackendError):
    """A database result was required but none was found."""


class MultipleResultsFoundError(BackendError):
    """A single database result was required but more than one were found."""


class InconsistentGraphError(BackendError):
    """Exception raised for inconsistencies found in the graph database."""


class GraphError(BackendError):
    """Graph error that can be constructed with more error metadata."""

    def __init__(
        self,
        *args: Any,  # noqa: ANN401
        errors: Sequence[ErrorDetails] = (),
        retryable: bool = False,
    ) -> None:
        """Construct a new ingestion failure with underlying details."""
        super().__init__(*args)
        self._errors = list(errors)
        self._retryable = retryable

    def errors(self) -> list[ErrorDetails]:
        """Details about underlying errors."""
        return self._errors

    def is_retryable(self) -> bool:
        """Whether the error is retryable."""
        return self._retryable


class IngestionError(GraphError):
    """Error for ingestion failures with underlying details."""


class MatchingError(GraphError):
    """Exception raised for errors during the matching process."""


class DeletionFailedError(GraphError):
    """Exception raised when the the specified item could not be deleted."""
