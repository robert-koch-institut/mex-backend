from typing import Any

import pytest

from mex.backend.merged.helpers import (
    search_merged_items_in_graph,
)
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
def test_search_merged_items_in_graph_mocked(
    mocked_graph_result: list[dict[str, Any]], expected: Any, mocked_graph: MockedGraph
) -> None:
    mocked_graph.return_value = mocked_graph_result

    try:
        merged_result = search_merged_items_in_graph(identifier="bFQoRhcVH5DHUB")
    except Exception as error:
        if str(expected) not in str(error):
            raise AssertionError(expected) from error
    else:
        assert merged_result.model_dump(exclude_defaults=True) == expected
