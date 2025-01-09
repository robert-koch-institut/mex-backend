import pytest

from mex.backend.extracted.models import ExtractedItemSearch
from mex.backend.graph.connector import MEX_EXTRACTED_PRIMARY_SOURCE, GraphConnector
from mex.common.models import AnyExtractedModel


@pytest.mark.integration
def test_graph_ingest_and_query_roundtrip(
    load_dummy_data: dict[str, AnyExtractedModel],
) -> None:
    seeded_models = sorted(
        [*load_dummy_data.values(), MEX_EXTRACTED_PRIMARY_SOURCE],
        key=lambda x: x.identifier,
    )

    connector = GraphConnector.get()
    result = connector.fetch_extracted_items(None, None, None, 0, len(seeded_models))

    expected = ExtractedItemSearch(
        items=[e.model_dump() for e in seeded_models], total=len(seeded_models)
    )
    fetched = ExtractedItemSearch(items=result["items"], total=result["total"])

    assert fetched == expected
