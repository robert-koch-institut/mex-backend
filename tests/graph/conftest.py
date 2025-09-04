from typing import Any
from unittest.mock import call

import pytest

from mex.backend.graph.models import IngestParams


@pytest.fixture
def organizational_unit_rule_set_ingest_result() -> list[list[dict[str, Any]]]:
    return [
        [
            {
                "identifier": None,
                "stableTargetId": "bFQoRhcVH5DHU6",
                "entityType": "AdditiveOrganizationalUnit",
                "linkRels": [
                    {
                        "nodeLabels": ["MergedOrganizationalUnit"],
                        "nodeProps": {"identifier": "cWWm02l1c6cucKjIhkFqY4"},
                        "edgeLabel": "parentUnit",
                        "edgeProps": {"position": 0},
                    }
                ],
                "createRels": [
                    {
                        "nodeLabels": ["Text"],
                        "nodeProps": {"value": "Unit 1.7", "language": "en"},
                        "edgeLabel": "name",
                        "edgeProps": {"position": 0},
                    },
                    {
                        "nodeLabels": ["Link"],
                        "nodeProps": {
                            "title": "Unit Homepage",
                            "url": "https://unit-1-7",
                            "language": None,
                        },
                        "edgeLabel": "website",
                        "edgeProps": {"position": 0},
                    },
                ],
                "nodeProps": {},
            }
        ],
        [
            {
                "identifier": None,
                "stableTargetId": "bFQoRhcVH5DHU6",
                "entityType": "SubtractiveOrganizationalUnit",
                "nodeProps": {},
            }
        ],
        [
            {
                "identifier": None,
                "stableTargetId": "bFQoRhcVH5DHU6",
                "entityType": "PreventiveOrganizationalUnit",
                "nodeProps": {},
            }
        ],
    ]


def organizational_unit_rule_set_ingest_call_expectation() -> list[object]:
    return [
        call(
            call(
                "merge_item",
                params=IngestParams(
                    merged_label="MergedOrganizationalUnit",
                    node_label="AdditiveOrganizationalUnit",
                    all_referenced_labels=[
                        "MergedOrganization",
                        "MergedOrganizationalUnit",
                    ],
                    all_nested_labels=["Link", "Text"],
                    detach_node_edges=["parentUnit", "unitOf"],
                    delete_node_edges=[
                        "alternativeName",
                        "name",
                        "shortName",
                        "website",
                    ],
                    has_link_rels=True,
                    has_create_rels=True,
                ),
            ),
            data={
                "stableTargetId": "bFQoRhcVH5DHU6",
                "identifier": None,
                "entityType": "AdditiveOrganizationalUnit",
                "nodeProps": {"email": []},
                "linkRels": [
                    {
                        "nodeLabels": ["MergedOrganizationalUnit"],
                        "nodeProps": {"identifier": "cWWm02l1c6cucKjIhkFqY4"},
                        "edgeLabel": "parentUnit",
                        "edgeProps": {"position": 0},
                    }
                ],
                "createRels": [
                    {
                        "nodeLabels": ["Text"],
                        "nodeProps": {"value": "Unit 1.7", "language": "en"},
                        "edgeLabel": "name",
                        "edgeProps": {"position": 0},
                    },
                    {
                        "nodeLabels": ["Link"],
                        "nodeProps": {
                            "language": None,
                            "title": "Unit Homepage",
                            "url": "https://unit-1-7",
                        },
                        "edgeLabel": "website",
                        "edgeProps": {"position": 0},
                    },
                ],
            },
        ),
        call(
            call(
                "merge_item",
                params=IngestParams(
                    merged_label="MergedOrganizationalUnit",
                    node_label="SubtractiveOrganizationalUnit",
                    all_referenced_labels=[
                        "MergedOrganization",
                        "MergedOrganizationalUnit",
                    ],
                    all_nested_labels=["Link", "Text"],
                    detach_node_edges=["parentUnit", "unitOf"],
                    delete_node_edges=[
                        "alternativeName",
                        "name",
                        "shortName",
                        "website",
                    ],
                    has_link_rels=True,
                    has_create_rels=True,
                ),
            ),
            data={
                "stableTargetId": "bFQoRhcVH5DHU6",
                "identifier": None,
                "entityType": "SubtractiveOrganizationalUnit",
                "nodeProps": {"email": []},
                "linkRels": [],
                "createRels": [],
            },
        ),
        call(
            call(
                "merge_item",
                params=IngestParams(
                    merged_label="MergedOrganizationalUnit",
                    node_label="PreventiveOrganizationalUnit",
                    all_referenced_labels=["MergedPrimarySource"],
                    all_nested_labels=[],
                    detach_node_edges=[
                        "alternativeName",
                        "email",
                        "name",
                        "parentUnit",
                        "shortName",
                        "unitOf",
                        "website",
                    ],
                    delete_node_edges=[],
                    has_link_rels=True,
                    has_create_rels=False,
                ),
            ),
            data={
                "stableTargetId": "bFQoRhcVH5DHU6",
                "identifier": None,
                "entityType": "PreventiveOrganizationalUnit",
                "nodeProps": {},
                "linkRels": [],
                "createRels": [],
            },
        ),
    ]
