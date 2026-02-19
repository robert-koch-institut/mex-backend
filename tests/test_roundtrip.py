from typing import TYPE_CHECKING, cast

import pytest

from mex.backend.graph.models import MExPrimarySource
from mex.backend.merged.helpers import search_merged_items_in_graph
from mex.common.merged.main import create_merged_item
from mex.common.models import (
    MEX_PRIMARY_SOURCE_STABLE_TARGET_ID,
    AnyExtractedModel,
    AnyMergedModel,
    AnyRuleSetResponse,
    ExtractedPrimarySource,
    PaginatedItemsContainer,
)
from mex.common.types import Validation

if TYPE_CHECKING:  # pragma: no cover
    from tests.conftest import DummyData


@pytest.fixture
def merged_dummy_data(dummy_data: DummyData) -> dict[str, AnyMergedModel]:
    """Merge the manually created `dummy_data` into merged items."""

    def _merge_single(item: AnyExtractedModel | AnyRuleSetResponse) -> AnyMergedModel:
        assert isinstance(item, AnyExtractedModel)
        return create_merged_item(item.stableTargetId, [item], None, Validation.STRICT)

    return {
        "primary_source_1": _merge_single(dummy_data["primary_source_1"]),
        "primary_source_2": _merge_single(dummy_data["primary_source_2"]),
        "contact_point_1": _merge_single(dummy_data["contact_point_1"]),
        "contact_point_2": _merge_single(dummy_data["contact_point_2"]),
        "organization_1": _merge_single(dummy_data["organization_1"]),
        "organization_2": _merge_single(dummy_data["organization_2"]),
        "unit_1": _merge_single(dummy_data["unit_1"]),
        "unit_2": create_merged_item(
            dummy_data["unit_2"].stableTargetId,
            [dummy_data["unit_2"]],
            dummy_data["unit_2_rule_set"],
            Validation.STRICT,
        ),
        "unit_3": create_merged_item(
            dummy_data["unit_3_standalone_rule_set"].stableTargetId,
            [],
            dummy_data["unit_3_standalone_rule_set"],
            Validation.STRICT,
        ),
        "activity_1": _merge_single(dummy_data["activity_1"]),
    }


@pytest.mark.integration
@pytest.mark.usefixtures("loaded_dummy_data")
def test_graph_ingest_and_query_roundtrip(
    merged_dummy_data: dict[str, AnyMergedModel],
) -> None:
    seeded_and_dummy = [
        *merged_dummy_data.values(),
        create_merged_item(
            MEX_PRIMARY_SOURCE_STABLE_TARGET_ID,
            [cast("ExtractedPrimarySource", MExPrimarySource())],
            None,
            Validation.STRICT,
        ),
    ]
    expected_models = sorted(seeded_and_dummy, key=lambda x: x.identifier)

    fetched_container = search_merged_items_in_graph(
        limit=len(expected_models),
    )
    expected_container = PaginatedItemsContainer[AnyMergedModel](
        items=[e.model_dump() for e in expected_models], total=len(expected_models)
    )

    assert fetched_container.model_dump() == expected_container.model_dump()
