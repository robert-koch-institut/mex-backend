from mex.common.exceptions import MExError

class NoResultFound(MExError):
    """A database result was required but none was found."""

    # XXX Akin to sqlalchemy.exc.NoResultFound


class MultipleResultsFound(MExError):
    """A single database result was required but more than one were found."""

    # XXX Akin to sqlalchemy.exc.MultipleResultsFound
