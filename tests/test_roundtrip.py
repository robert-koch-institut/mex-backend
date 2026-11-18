from collections import deque
from typing import TYPE_CHECKING, cast

import pytest

from mex.backend.extracted.helpers import get_extracted_item_from_graph
from mex.backend.graph.connector import GraphConnector
from mex.backend.graph.models import (
    MEX_EDITOR_PRIMARY_SOURCE,
    MEX_PRIMARY_SOURCE,
)
from mex.backend.merged.helpers import search_merged_items_in_graph
from mex.common.merged.main import create_merged_item
from mex.common.models import (
    MEX_EDITOR_PRIMARY_SOURCE_STABLE_TARGET_ID,
    MEX_PRIMARY_SOURCE_STABLE_TARGET_ID,
    AnyExtractedModel,
    AnyMergedModel,
    AnyRuleSetResponse,
    ExtractedPrimarySource,
    ExtractedResource,
    PaginatedItemsContainer,
)
from mex.common.types import (
    AccessRestriction,
    Link,
    ResourceCreationMethod,
    Text,
    TextLanguage,
    Theme,
    Validation,
)

if TYPE_CHECKING:
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
            [cast("ExtractedPrimarySource", MEX_PRIMARY_SOURCE)],
            None,
            Validation.STRICT,
        ),
        create_merged_item(
            MEX_EDITOR_PRIMARY_SOURCE_STABLE_TARGET_ID,
            [cast("ExtractedPrimarySource", MEX_EDITOR_PRIMARY_SOURCE)],
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


@pytest.mark.integration
def test_graph_ingest_and_query_roundtrip_mixed_nested_types(
    loaded_dummy_data: DummyData,
) -> None:
    connector = GraphConnector.get()
    resource = ExtractedResource(
        accessRestriction=AccessRestriction["OPEN"],
        contact=[loaded_dummy_data["contact_point_1"].stableTargetId],
        hadPrimarySource=loaded_dummy_data["primary_source_1"].stableTargetId,
        hasLegalBasis=[
            Text(value="Infektionsschutzgesetz", language=TextLanguage.DE),
            Link(title="IfSG", url="https://www.gesetze-im-internet.de/ifsg"),
        ],
        identifierInPrimarySource="r-1",
        resourceCreationMethod=[ResourceCreationMethod["DISEASE_SURVEILLANCE"]],
        theme=[Theme["INFECTIOUS_DISEASES_AND_EPIDEMIOLOGY"]],
        title=[Text(value="Resource 1", language=TextLanguage.EN)],
        unitInCharge=[loaded_dummy_data["unit_1"].stableTargetId],
    )
    deque(connector.ingest_items([resource]))
    assert get_extracted_item_from_graph(resource.identifier) == resource

    # swap the types at each position: Text becomes Link and vice versa
    swapped = resource.model_copy(
        update={
            "hasLegalBasis": [
                Link(url="https://www.gesetze-im-internet.de/ifsg"),
                Text(value="Infection Protection Act", language=TextLanguage.EN),
            ]
        }
    )
    deque(connector.ingest_items([swapped]))
    assert get_extracted_item_from_graph(resource.identifier) == swapped

    # the replaced nested nodes are gone, not just disconnected
    result = connector.driver.execute_query(
        "MATCH (n:Text|Link) WHERE NOT (n)--() RETURN count(n) AS orphans"
    )
    assert result.records[0]["orphans"] == 0
