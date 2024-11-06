from typing import Protocol

from pydantic import ValidationError
from pydantic_core import ErrorDetails

from mex.common.exceptions import MExError


class DetailedError(Protocol):
    """Protocol for errors that offer details."""

    def errors(self) -> list[ErrorDetails]:
        """Details about each underlying error."""


class NoResultFoundError(MExError):
    """A database result was required but none was found."""


class MultipleResultsFoundError(MExError):
    """A single database result was required but more than one were found."""


class InconsistentGraphError(MExError):
    """Exception raised for inconsistencies found in the graph database."""

    def errors(self) -> list[ErrorDetails]:
        """Details about each underlying error."""
        if isinstance(self.__cause__, ValidationError):
            return self.__cause__.errors()
        return []
