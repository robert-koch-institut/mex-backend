from mex.backend.exceptions import BackendError


class NoResultFoundError(BackendError):
    """A database result was required but none was found."""


class MultipleResultsFoundError(BackendError):
    """A single database result was required but more than one were found."""


class InconsistentGraphError(BackendError):
    """Exception raised for inconsistencies found in the graph database."""
