from typing import Any

import pytest
from fastapi.testclient import TestClient
from starlette import status

from mex.common.models import ExtractedOrganizationalUnit
from tests.conftest import MockedGraph


def test_search_merged_items_mocked(
    client_with_api_key_read_permission: TestClient, mocked_graph: MockedGraph
) -> None:
    unit = ExtractedOrganizationalUnit.model_validate(
        {
            "hadPrimarySource": "2222222222222222",
            "identifierInPrimarySource": "unit-1",
            "email": ["test@foo.bar"],
            "name": [
                {"value": "Eine unit von einer Org.", "language": "de"},
                {"value": "A unit of an org.", "language": "en"},
            ],
        }
    )
    mocked_graph.return_value = [
        {
            "items": [
                {
                    "_components": [
                        {
                            "identifier": unit.identifier,
                            "identifierInPrimarySource": unit.identifierInPrimarySource,
                            "stableTargetId": unit.stableTargetId,
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
                                    "label": "name",
                                    "position": 1,
                                    "value": {
                                        "value": "A unit of an org.",
                                        "language": "en",
                                    },
                                },
                            ],
                        }
                    ],
                    "entityType": "MergedOrganizationalUnit",
                    "identifier": unit.stableTargetId,
                }
            ],
            "total": 14,
        }
    ]

    response = client_with_api_key_read_permission.get("/v0/merged-item")
    assert response.status_code == status.HTTP_200_OK, response.text
    assert response.json() == {
        "items": [
            {
                "$type": "MergedOrganizationalUnit",
                "alternativeName": [],
                "email": ["test@foo.bar"],
                "identifier": unit.stableTargetId,
                "name": [
                    {"language": "de", "value": "Eine unit von einer Org."},
                    {"language": "en", "value": "A unit of an org."},
                ],
                "parentUnit": None,
                "shortName": [],
                "unitOf": [],
                "website": [],
            }
        ],
        "total": 14,
    }


@pytest.mark.parametrize(
    ("query_string", "expected"),
    [
        (
            "?limit=1",
            {
                "items": [
                    {
                        "$type": "MergedPrimarySource",
                        "alternativeTitle": [],
                        "contact": [],
                        "description": [],
                        "documentation": [],
                        "identifier": "00000000000000",
                        "locatedAt": [],
                        "title": [],
                        "unitInCharge": [],
                        "version": None,
                    }
                ],
                "total": 9,
            },
        ),
        (
            "?limit=1&skip=8",
            {
                "items": [
                    {
                        "email": ["info@contact-point.one"],
                        "$type": "MergedContactPoint",
                        "identifier": "bFQoRhcVH5DHUz",
                    }
                ],
                "total": 9,
            },
        ),
        (
            "?entityType=MergedContactPoint",
            {
                "items": [
                    {
                        "$type": "MergedContactPoint",
                        "email": ["help@contact-point.two"],
                        "identifier": "bFQoRhcVH5DHUB",
                    },
                    {
                        "$type": "MergedContactPoint",
                        "email": ["info@contact-point.one"],
                        "identifier": "bFQoRhcVH5DHUz",
                    },
                ],
                "total": 2,
            },
        ),
        (
            "?q=cool",
            {
                "items": [
                    {
                        "$type": "MergedPrimarySource",
                        "alternativeTitle": [],
                        "contact": [],
                        "description": [],
                        "documentation": [],
                        "identifier": "bFQoRhcVH5DHUt",
                        "locatedAt": [],
                        "title": [],
                        "unitInCharge": [],
                        "version": "Cool Version v2.13",
                    }
                ],
                "total": 1,
            },
        ),
        (
            "?identifier=bFQoRhcVH5DHUx",
            {
                "items": [
                    {
                        "$type": "MergedOrganizationalUnit",
                        "alternativeName": [],
                        "email": [],
                        "identifier": "bFQoRhcVH5DHUx",
                        "name": [{"language": "en", "value": "Unit 1"}],
                        "parentUnit": None,
                        "shortName": [],
                        "unitOf": ["bFQoRhcVH5DHUv"],
                        "website": [],
                    },
                ],
                "total": 1,
            },
        ),
        (
            "?identifier=bFQoRhcVH5DHUF",
            {
                "items": [
                    {
                        "$type": "MergedOrganizationalUnit",
                        "alternativeName": [],
                        "email": [],
                        "identifier": "bFQoRhcVH5DHUF",
                        "name": [
                            {"language": "en", "value": "Unit 1.6"},
                            {"language": "en", "value": "Unit 1.7"},
                        ],
                        "parentUnit": "bFQoRhcVH5DHUx",
                        "shortName": [],
                        "unitOf": ["bFQoRhcVH5DHUv"],
                        "website": [
                            {
                                "language": None,
                                "title": "Unit Homepage",
                                "url": "https://unit-1-7",
                            }
                        ],
                    }
                ],
                "total": 1,
            },
        ),
        ("?identifier=thisIdDoesNotExist", {"items": [], "total": 0}),
        ("?q=queryNotFound", {"items": [], "total": 0}),
    ],
    ids=[
        "limit 1",
        "skip 1",
        "entity type contact points",
        "full text search",
        "identifier filter",
        "identifier filter with composite result",
        "identifier not found",
        "full text not found",
    ],
)
@pytest.mark.usefixtures("load_dummy_data", "load_dummy_rule_set")
@pytest.mark.integration
def test_search_merged_items(
    client_with_api_key_read_permission: TestClient,
    query_string: str,
    expected: dict[str, Any],
) -> None:
    response = client_with_api_key_read_permission.get(f"/v0/merged-item{query_string}")
    assert response.status_code == status.HTTP_200_OK, response.text
    assert response.json() == expected
