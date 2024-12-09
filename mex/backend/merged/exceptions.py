from mex.backend.exceptions import BackendError


class MergingError(BackendError):
    """Creating a merged item from extracted items and rules failed."""
