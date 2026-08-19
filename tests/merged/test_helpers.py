from typing import TYPE_CHECKING, Any

import pytest
from pytest import LogCaptureFixture

from mex.backend.exceptions import BackendError
from mex.backend.graph.exceptions import MergingError
from mex.backend.merged.helpers import (
    delete_merged_item_from_graph,
    get_merged_item_from_graph,
    merge_items_in_graph,
    search_merged_items_in_graph,
)
from mex.backend.rules.helpers import get_rule_set_from_graph
from mex.common.merged.main import create_merged_item
from mex.common.models import (
    MergedActivity,
    MergedOrganization,
    MergedOrganizationalUnit,
    OrganizationalUnitRuleSetResponse,
    OrganizationRuleSetResponse,
    PreviewOrganizationalUnit,
)
from mex.common.types import Identifier, TextLanguage, Validation

if TYPE_CHECKING:  # pragma: no cover
    from tests.conftest import DummyData, MockedGraph


@pytest.mark.usefixtures("loaded_dummy_data")
@pytest.mark.integration
def test_search_merged_items_in_graph() -> None:
    merged_result = search_merged_items_in_graph(
        identifier="bFQoRhcVH5DHUF",
    )
    assert merged_result.model_dump(exclude_defaults=True) == {
        "items": [
            {
                "officialName": [
                    {"value": "RKI", "language": TextLanguage.DE},
                    {"value": "Robert Koch Institute", "language": TextLanguage.EN},
                ],
                "identifier": "bFQoRhcVH5DHUF",
            }
        ],
        "total": 1,
    }


@pytest.mark.parametrize(
    ("mocked_graph_result", "expected"),
    [
        pytest.param(
            [
                {
                    "items": [
                        {
                            "_components": [
                                {
                                    "identifier": "jbZ5Br9Vninm08ptYZFxW",
                                    "identifierInPrimarySource": "unit-1",
                                    "stableTargetId": "e5rfAc2p5zV39WUVZeAR1",
                                    "email": ["test@foo.bar"],
                                    "entityType": "ExtractedOrganizationalUnit",
                                    "_refs": [
                                        {
                                            "label": "hadPrimarySource",
                                            "position": 0,
                                            "value": "2222222222222222",
                                        },
                                        {
                                            "label": "name",
                                            "position": 0,
                                            "value": {
                                                "value": "Eine unit von einer Org.",
                                                "language": "de",
                                            },
                                        },
                                    ],
                                }
                            ],
                            "entityType": "MergedOrganizationalUnit",
                            "identifier": "e5rfAc2p5zV39WUVZeAR1",
                        }
                    ],
                    "total": 1,
                }
            ],
            {
                "items": [
                    {
                        "email": ["test@foo.bar"],
                        "identifier": "e5rfAc2p5zV39WUVZeAR1",
                        "name": [
                            {"language": "de", "value": "Eine unit von einer Org."},
                        ],
                    }
                ],
                "total": 1,
            },
            id="no-rules",
        ),
        pytest.param(
            [
                {
                    "items": [
                        {
                            "_components": [
                                {
                                    "identifier": "jbZ5Br9Vninm08ptYZFxW",
                                    "identifierInPrimarySource": "unit-1",
                                    "stableTargetId": "e5rfAc2p5zV39WUVZeAR1",
                                    "email": ["test@foo.bar"],
                                    "entityType": "ExtractedOrganizationalUnit",
                                    "_refs": [
                                        {
                                            "label": "hadPrimarySource",
                                            "position": 0,
                                            "value": "2222222222222222",
                                        },
                                        {
                                            "label": "name",
                                            "position": 0,
                                            "value": {
                                                "value": "Eine unit von einer Org.",
                                                "language": "de",
                                            },
                                        },
                                    ],
                                },
                                {
                                    "stableTargetId": "e5rfAc2p5zV39WUVZeAR1",
                                    "email": ["bar@foo.bar"],
                                    "entityType": "AdditiveOrganizationalUnit",
                                    "_refs": [],
                                },
                            ],
                            "entityType": "MergedOrganizationalUnit",
                            "identifier": "e5rfAc2p5zV39WUVZeAR1",
                        }
                    ],
                    "total": 1,
                }
            ],
            "inconsistent number of rules found: 1",
            id="one-rule-raises-error",
        ),
        pytest.param(
            [
                {
                    "items": [
                        {
                            "_components": [
                                {
                                    "identifier": "jbZ5Br9Vninm08ptYZFxW",
                                    "identifierInPrimarySource": "unit-1",
                                    "email": ["test@foo.bar"],
                                    "entityType": "ExtractedOrganizationalUnit",
                                    "_refs": [
                                        {
                                            "label": "hadPrimarySource",
                                            "position": 0,
                                            "value": "2222222222222222",
                                        },
                                        {
                                            "label": "name",
                                            "position": 0,
                                            "value": {
                                                "value": "Eine unit von einer Org.",
                                                "language": "de",
                                            },
                                        },
                                        {
                                            "label": "stableTargetId",
                                            "position": 0,
                                            "value": "e5rfAc2p5zV39WUVZeAR1",
                                        },
                                    ],
                                },
                                {
                                    "email": ["bar@foo.bar"],
                                    "entityType": "AdditiveOrganizationalUnit",
                                    "_refs": [
                                        {
                                            "label": "stableTargetId",
                                            "position": 0,
                                            "value": "e5rfAc2p5zV39WUVZeAR1",
                                        },
                                    ],
                                },
                                {
                                    "email": ["test@foo.bar"],
                                    "entityType": "SubtractiveOrganizationalUnit",
                                    "_refs": [
                                        {
                                            "label": "stableTargetId",
                                            "position": 0,
                                            "value": "e5rfAc2p5zV39WUVZeAR1",
                                        },
                                    ],
                                },
                                {
                                    "entityType": "PreventiveOrganizationalUnit",
                                    "_refs": [
                                        {
                                            "label": "stableTargetId",
                                            "position": 0,
                                            "value": "e5rfAc2p5zV39WUVZeAR1",
                                        },
                                    ],
                                },
                                {
                                    "entityType": "WorkflowOrganizationalUnit",
                                    "_refs": [
                                        {
                                            "label": "stableTargetId",
                                            "position": 0,
                                            "value": "e5rfAc2p5zV39WUVZeAR1",
                                        },
                                    ],
                                },
                            ],
                            "entityType": "MergedOrganizationalUnit",
                            "identifier": "e5rfAc2p5zV39WUVZeAR1",
                        }
                    ],
                    "total": 1,
                }
            ],
            {
                "items": [
                    {
                        "email": ["bar@foo.bar"],
                        "identifier": "e5rfAc2p5zV39WUVZeAR1",
                        "name": [
                            {"language": "de", "value": "Eine unit von einer Org."},
                        ],
                    }
                ],
                "total": 1,
            },
            id="four-rules",
        ),
    ],
)
@pytest.mark.usefixtures("mocked_valkey")
def test_search_merged_items_in_graph_mocked(
    mocked_graph_result: list[dict[str, Any]],
    expected: Any,  # noqa: ANN401
    mocked_graph: MockedGraph,
) -> None:
    mocked_graph.return_value = mocked_graph_result

    try:
        merged_result = search_merged_items_in_graph(identifier="bFQoRhcVH5DHUB")
    except Exception as error:
        if str(expected) not in str(error):
            raise AssertionError(expected) from error
    else:
        assert merged_result.model_dump(exclude_defaults=True) == expected


@pytest.mark.xfail(reason="stopgap mx-1530")
@pytest.mark.integration
def test_get_merged_item_from_graph(
    loaded_dummy_data: DummyData,
) -> None:
    organization_1 = loaded_dummy_data["organization_1"]
    organization_2 = loaded_dummy_data["organization_2"]
    fetched = get_merged_item_from_graph(organization_1.stableTargetId)
    expected = create_merged_item(
        identifier=organization_1.stableTargetId,
        extracted_items=[organization_2, organization_1],
        rule_set=None,
        validation=Validation.STRICT,
    )
    assert fetched.model_dump() == expected.model_dump()


@pytest.mark.integration
def test_get_merged_item_from_graph_not_found() -> None:
    with pytest.raises(BackendError, match="Merged item was not found"):
        get_merged_item_from_graph(Identifier("notARealIdentifier"))


@pytest.mark.integration
def test_delete_merged_item_from_graph_not_found() -> None:
    # Expect deletion fails for non-existent item
    with pytest.raises(BackendError, match="Merged item was not found"):
        delete_merged_item_from_graph(Identifier("notARealIdentifier"))


@pytest.mark.integration
def test_delete_merged_item_from_graph_inbound_connections(
    loaded_dummy_data: DummyData,
) -> None:
    # Use item with inbound connections
    extracted_item = loaded_dummy_data["organization_1"]

    # Expect function call fails
    with pytest.raises(BackendError, match=r"Deletion of MergedItem.* failed"):
        delete_merged_item_from_graph(extracted_item.stableTargetId)

    # Verify item is still here
    merged_item = get_merged_item_from_graph(extracted_item.stableTargetId)
    assert extracted_item.stableTargetId == merged_item.identifier


@pytest.mark.integration
def test_delete_merged_item_from_graph(
    loaded_dummy_data: DummyData,
    caplog: LogCaptureFixture,
) -> None:
    # Use item without inbound connections
    extracted_item = loaded_dummy_data["unit_2"]
    merged_item = get_merged_item_from_graph(extracted_item.stableTargetId)
    assert extracted_item.stableTargetId == merged_item.identifier

    # Call the function
    delete_merged_item_from_graph(extracted_item.stableTargetId)

    # Verify logging occurred with expected content
    assert f"deleted item {extracted_item.stableTargetId}" in caplog.text
    assert "deleted_merged_count" in caplog.text

    # Verify item is gone
    with pytest.raises(BackendError, match="Merged item was not found"):
        get_merged_item_from_graph(extracted_item.stableTargetId)


@pytest.mark.integration
def test_merge_items_in_graph(loaded_dummy_data: DummyData) -> None:
    # `stableTargetId` is computed by the identity provider on every access, so the
    # goner's id has to be captured before the merge re-points it at the keeper
    goner_id = loaded_dummy_data["organization_1"].stableTargetId
    keeper_id = loaded_dummy_data["organization_2"].stableTargetId

    merge_items_in_graph(goner_id, keeper_id)

    # the keeper absorbed the goner's extracted item, so it now carries both names
    merged_keeper = get_merged_item_from_graph(keeper_id)
    assert isinstance(merged_keeper, MergedOrganization)
    assert sorted(text.value for text in merged_keeper.officialName) == [
        "RKI",
        "Robert Koch Institute",
    ]

    # the units that referenced the goner now reference the keeper
    units = search_merged_items_in_graph(
        entity_type=["MergedOrganizationalUnit"],
        validation=Validation.LENIENT,
    )
    assert {
        unit_of
        for unit in units.items
        if isinstance(unit, PreviewOrganizationalUnit | MergedOrganizationalUnit)
        for unit_of in unit.unitOf
    } == {keeper_id}

    # the goner is left behind as a tombstone pointing at the keeper
    rule_set = get_rule_set_from_graph(goner_id)
    assert isinstance(rule_set, OrganizationRuleSetResponse)
    assert rule_set.additive.supersededBy == keeper_id
    assert rule_set.additive.officialName == []


@pytest.mark.integration
def test_merge_items_in_graph_drops_self_references(
    loaded_dummy_data: DummyData,
) -> None:
    goner_id = loaded_dummy_data["unit_1"].stableTargetId
    keeper_id = loaded_dummy_data["unit_2"].stableTargetId

    merge_items_in_graph(goner_id, keeper_id)

    # the keeper's rule set pointed its parentUnit at the goner, which would have
    # become a self-reference, so it is dropped instead of redirected
    keeper_rule_set = get_rule_set_from_graph(keeper_id)
    assert isinstance(keeper_rule_set, OrganizationalUnitRuleSetResponse)
    assert keeper_rule_set.additive.parentUnit is None

    # an unrelated rule set that pointed at the goner is redirected to the keeper
    standalone_rule_set = get_rule_set_from_graph(Identifier("StandaloneRule"))
    assert isinstance(standalone_rule_set, OrganizationalUnitRuleSetResponse)
    assert standalone_rule_set.additive.parentUnit == keeper_id


@pytest.mark.integration
def test_merge_items_in_graph_deduplicates_references(
    loaded_dummy_data: DummyData,
) -> None:
    # activity_1 references unit_1 twice: once as contact and once as responsibleUnit
    activity_id = loaded_dummy_data["activity_1"].stableTargetId
    goner_id = loaded_dummy_data["contact_point_1"].stableTargetId
    keeper_id = loaded_dummy_data["contact_point_2"].stableTargetId
    unit_id = loaded_dummy_data["unit_1"].stableTargetId

    merge_items_in_graph(goner_id, keeper_id)

    merged_activity = get_merged_item_from_graph(activity_id)
    assert isinstance(merged_activity, MergedActivity)
    # both contact points collapse into a single reference to the keeper
    assert merged_activity.contact == [keeper_id, unit_id]


@pytest.mark.integration
def test_merge_items_in_graph_type_mismatch_error(
    loaded_dummy_data: DummyData,
) -> None:
    goner_id = loaded_dummy_data["organization_1"].stableTargetId
    keeper_id = loaded_dummy_data["unit_1"].stableTargetId

    with pytest.raises(MergingError, match="Violated: same_merged_type"):
        merge_items_in_graph(goner_id, keeper_id)
