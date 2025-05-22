from typing import Any, cast

import pytest
from fastapi.testclient import TestClient
from starlette import status

from mex.backend.rules.helpers import update_and_get_rule_set
from mex.common.models import (
    AnyExtractedModel,
    ExtractedOrganizationalUnit,
    OrganizationalUnitRuleSetRequest,
    SubtractiveOrganizationalUnit,
)
from tests.conftest import MockedGraph


@pytest.mark.usefixtures("mocked_redis")
def test_search_merged_items_mocked(
    client_with_api_key_read_permission: TestClient,
    mocked_graph: MockedGraph,
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
        (
            "?hadPrimarySource=bFQoRhcVH5DHUt",
            {
                "items": [
                    {
                        "$type": "MergedOrganization",
                        "alternativeName": [],
                        "geprisId": [],
                        "gndId": [],
                        "identifier": "bFQoRhcVH5DHUv",
                        "isniId": [],
                        "officialName": [
                            {"language": "de", "value": "RKI"},
                            {"language": "en", "value": "Robert Koch Institute"},
                            {
                                "language": "de",
                                "value": "Robert Koch Institut ist the best",
                            },
                        ],
                        "rorId": [],
                        "shortName": [],
                        "viafId": [],
                        "wikidataId": [],
                    },
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
                    },
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
                "total": 3,
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
        "had primary source filter",
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


@pytest.mark.integration
def test_search_merged_items_skip_on_validation_error(
    client_with_api_key_read_permission: TestClient,
    load_dummy_data: dict[str, AnyExtractedModel],
) -> None:
    response = client_with_api_key_read_permission.get(
        "/v0/merged-item?entityType=MergedOrganizationalUnit"
    )
    assert response.status_code == status.HTTP_200_OK, response.text
    assert len(response.json()["items"]) == 2

    # remove the name from org unit 2 to make it invalid
    organizational_unit_2 = cast(
        "ExtractedOrganizationalUnit", load_dummy_data["organizational_unit_2"]
    )
    update_and_get_rule_set(
        stable_target_id=organizational_unit_2.stableTargetId,
        rule_set=OrganizationalUnitRuleSetRequest(
            subtractive=SubtractiveOrganizationalUnit(name=organizational_unit_2.name)
        ),
    )
    response = client_with_api_key_read_permission.get(
        "/v0/merged-item?entityType=MergedOrganizationalUnit"
    )
    assert response.status_code == status.HTTP_200_OK, response.text
    # expect org unit 2 to be filtered out (only unit 1 remains)
    assert response.json()["items"] == [
        {
            "parentUnit": None,
            "name": [{"value": "Unit 1", "language": "en"}],
            "alternativeName": [],
            "email": [],
            "shortName": [],
            "unitOf": ["bFQoRhcVH5DHUv"],
            "website": [],
            "$type": "MergedOrganizationalUnit",
            "identifier": "bFQoRhcVH5DHUx",
        }
    ]
