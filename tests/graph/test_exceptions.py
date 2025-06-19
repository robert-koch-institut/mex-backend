from pydantic_core import ErrorDetails

from mex.backend.graph.exceptions import IngestionError


def test_ingestion_error() -> None:
    details = ErrorDetails(type="foo", loc=(9, 9), msg="danger", input=None)
    error = IngestionError("Something is wrong", errors=(details,), retryable=True)
    assert error.args[0] == "Something is wrong"
    assert error.is_retryable()
    assert error.errors() == [details]
