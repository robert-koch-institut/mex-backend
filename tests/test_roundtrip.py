import pytest

from mex.backend.extracted.helpers import search_extracted_items_in_graph
from mex.backend.graph.models import MExPrimarySource
from mex.common.models import AnyExtractedModel, PaginatedItemsContainer


@pytest.mark.integration
def test_graph_ingest_and_query_roundtrip(
    load_dummy_data: dict[str, AnyExtractedModel],
) -> None:
    seeded_models = sorted(
        [*load_dummy_data.values(), MExPrimarySource()],
        key=lambda x: x.identifier,
    )

    fetched = search_extracted_items_in_graph(
        limit=len(seeded_models),
    )
    expected = PaginatedItemsContainer[AnyExtractedModel](
        items=[e.model_dump() for e in seeded_models], total=len(seeded_models)
    )

    assert fetched == expected
