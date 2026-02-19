from typing import TYPE_CHECKING, Any
from unittest.mock import call

import pytest

from mex.backend.graph.models import IngestParams

if TYPE_CHECKING:  # pragma: no cover
    from tests.conftest import DummyData


@pytest.fixture
def organizational_unit_rule_set_ingest_result(
    dummy_data: DummyData,
) -> list[list[dict[str, Any]]]:
    return [
        [
            {
                "identifier": None,
                "stableTargetId": dummy_data["unit_2_rule_set"].stableTargetId,
                "entityType": "AdditiveOrganizationalUnit",
                "linkRels": [
                    {
                        "nodeLabels": ["MergedOrganizationalUnit"],
                        "nodeProps": {
                            "identifier": dummy_data["unit_1"].stableTargetId
                        },
                        "edgeLabel": "parentUnit",
                        "edgeProps": {"position": 0},
                    }
                ],
                "createRels": [
                    {
                        "nodeLabels": ["Text"],
                        "nodeProps": {"value": "Abteilung 1.6", "language": "de"},
                        "edgeLabel": "name",
                        "edgeProps": {"position": 0},
                    },
                    {
                        "nodeLabels": ["Link"],
                        "nodeProps": {
                            "title": "Unit Homepage",
                            "url": "https://unit-1-6",
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
                "stableTargetId": dummy_data["unit_2_rule_set"].stableTargetId,
                "entityType": "SubtractiveOrganizationalUnit",
                "nodeProps": {},
                "createRels": [
                    {
                        "nodeLabels": ["Text"],
                        "nodeProps": {
                            "value": "Unit 1.6",
                            "language": "en",
                        },
                        "edgeLabel": "name",
                        "edgeProps": {"position": 0},
                    },
                ],
            }
        ],
        [
            {
                "identifier": None,
                "stableTargetId": dummy_data["unit_2_rule_set"].stableTargetId,
                "entityType": "PreventiveOrganizationalUnit",
                "nodeProps": {},
            }
        ],
    ]


@pytest.fixture
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
                    detach_node_edges=["parentUnit", "supersededBy", "unitOf"],
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
                "stableTargetId": "UVCwaVgI6ZnD8zavnBkdz",
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
                        "nodeProps": {"value": "Abteilung 1.6", "language": "de"},
                        "edgeLabel": "name",
                        "edgeProps": {"position": 0},
                    },
                    {
                        "nodeLabels": ["Link"],
                        "nodeProps": {
                            "language": None,
                            "title": "Unit Homepage",
                            "url": "https://unit-1-6",
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
                "stableTargetId": "UVCwaVgI6ZnD8zavnBkdz",
                "identifier": None,
                "entityType": "SubtractiveOrganizationalUnit",
                "nodeProps": {"email": []},
                "linkRels": [],
                "createRels": [
                    {
                        "nodeLabels": ["Text"],
                        "nodeProps": {"value": "Unit 1.6", "language": "en"},
                        "edgeLabel": "name",
                        "edgeProps": {"position": 0},
                    }
                ],
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
                "stableTargetId": "UVCwaVgI6ZnD8zavnBkdz",
                "identifier": None,
                "entityType": "PreventiveOrganizationalUnit",
                "nodeProps": {},
                "linkRels": [],
                "createRels": [],
            },
        ),
    ]
