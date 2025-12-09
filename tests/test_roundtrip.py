from typing import cast

import pytest

from mex.backend.graph.models import MExPrimarySource
from mex.backend.merged.helpers import search_merged_items_in_graph
from mex.common.merged.main import create_merged_item
from mex.common.models import (
    MEX_PRIMARY_SOURCE_STABLE_TARGET_ID,
    AnyMergedModel,
    ExtractedPrimarySource,
    PaginatedItemsContainer,
)
from mex.common.types import Validation


@pytest.mark.integration
@pytest.mark.usefixtures("loaded_dummy_data")
def test_graph_ingest_and_query_roundtrip(
    merged_dummy_data: dict[str, AnyMergedModel],
) -> None:
    seeded_models = sorted(
        [
            *merged_dummy_data.values(),
            create_merged_item(
                MEX_PRIMARY_SOURCE_STABLE_TARGET_ID,
                [cast("ExtractedPrimarySource", MExPrimarySource())],
                None,
                Validation.STRICT,
            ),
        ],
        key=lambda x: x.identifier,
    )

    fetched = search_merged_items_in_graph(
        limit=len(seeded_models),
    )
    expected = PaginatedItemsContainer[AnyMergedModel](
        items=[e.model_dump() for e in seeded_models], total=len(seeded_models)
    )

    assert fetched.model_dump() == expected.model_dump()
