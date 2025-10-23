from typing import Any

import pytest
from pytest import LogCaptureFixture

from mex.backend.exceptions import BackendError
from mex.backend.merged.helpers import (
    delete_merged_item_from_graph,
    get_merged_item_from_graph,
    search_merged_items_in_graph,
)
from mex.common.merged.main import create_merged_item
from mex.common.models import AnyExtractedModel
from mex.common.types import Identifier, Validation
from tests.conftest import MockedGraph


@pytest.mark.usefixtures("load_dummy_data", "load_dummy_rule_set")
@pytest.mark.integration
def test_search_merged_items_in_graph() -> None:
    merged_result = search_merged_items_in_graph(
        identifier="bFQoRhcVH5DHUF",
    )
    assert merged_result.model_dump(exclude_defaults=True) == {
        "items": [
            {
                "identifier": "bFQoRhcVH5DHUF",
                "name": [
                    {"language": "en", "value": "Unit 1.6"},
                    {"language": "en", "value": "Unit 1.7"},
                ],
                "parentUnit": "bFQoRhcVH5DHUx",
                "unitOf": ["bFQoRhcVH5DHUv"],
                "website": [{"title": "Unit Homepage", "url": "https://unit-1-7"}],
            },
        ],
        "total": 1,
    }


@pytest.mark.parametrize(
    ("mocked_graph_result", "expected"),
    [
        (
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
        ),
        (
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
        ),
        (
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
        ),
    ],
    ids=["no_rules", "one_rule_raises_error", "three_rules"],
)
@pytest.mark.usefixtures("mocked_redis")
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


@pytest.mark.integration
def test_get_merged_item_from_graph(
    load_dummy_data: dict[str, AnyExtractedModel],
) -> None:
    organization_1 = load_dummy_data["organization_1"]
    organization_2 = load_dummy_data["organization_2"]
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
    load_dummy_data: dict[str, AnyExtractedModel],
) -> None:
    # Use item with inbound connections
    extracted_item = load_dummy_data["organization_1"]

    # Expect function call fails
    with pytest.raises(BackendError, match=r"Deletion of MergedItem.* failed"):
        delete_merged_item_from_graph(extracted_item.stableTargetId)

    # Verify item is still here
    merged_item = get_merged_item_from_graph(extracted_item.stableTargetId)
    assert extracted_item.stableTargetId == merged_item.identifier


@pytest.mark.integration
def test_delete_merged_item_from_graph(
    load_dummy_data: dict[str, AnyExtractedModel],
    caplog: LogCaptureFixture,
) -> None:
    # Use item without inbound connections
    extracted_item = load_dummy_data["organizational_unit_2"]
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
