from mex.common.exceptions import MExError


class NoResultFoundError(MExError):
    """A database result was required but none was found."""


class MultipleResultsFoundError(MExError):
    """A single database result was required but more than one were found."""
