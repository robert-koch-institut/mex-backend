from mex.backend.ingest.models import _BaseBulkIngestRequest


class DummyBulkIngestRequest(_BaseBulkIngestRequest):
    things: list[str]
    stuff: list[str]


def test_bulk_ingest_request_get_all() -> None:
    dummy_request = DummyBulkIngestRequest(things=["a", "b", "c"], stuff=["d"])
    assert dummy_request.get_all() == ["a", "b", "c", "d"]
