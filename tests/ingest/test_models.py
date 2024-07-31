from mex.backend.ingest.models import BulkIngestRequest


class DummyBulkIngestRequest(BulkIngestRequest):
    items: list[str]


def test_bulk_ingest_request_get_all() -> None:
    dummy_request = DummyBulkIngestRequest(items=["a", "b", "c"])
    assert dummy_request.items == ["a", "b", "c"]
