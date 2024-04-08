from typing import Any

import pytest
from fastapi.testclient import TestClient

from mex.backend.transform import to_primitive
from mex.common.models import ExtractedOrganizationalUnit
from tests.conftest import MockedGraph


def test_search_extracted_items_mocked(
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
                            "value": {"value": "A unit of an org.", "language": "en"},
                        },
                    ],
                }
            ],
            "total": 14,
        }
    ]

    response = client_with_api_key_read_permission.get("/v0/extracted-item")
    assert response.status_code == 200, response.text
    assert response.json() == {"items": [to_primitive(unit)], "total": 14}


@pytest.mark.parametrize(
    ("query_string", "expected"),
    [
        (
            "?limit=1",
            {
                "items": [
                    {
                        "$type": "ExtractedPrimarySource",
                        "alternativeTitle": [],
                        "contact": [],
                        "description": [],
                        "documentation": [],
                        "hadPrimarySource": "00000000000000",
                        "identifier": "00000000000000",
                        "identifierInPrimarySource": "mex",
                        "locatedAt": [],
                        "stableTargetId": "00000000000000",
                        "title": [],
                        "unitInCharge": [],
                        "version": None,
                    }
                ],
                "total": 7,
            },
        ),
        (
            "?limit=1&skip=4",
            {
                "items": [
                    {
                        "$type": "ExtractedContactPoint",
                        "email": ["info@contact-point.one"],
                        "hadPrimarySource": "bFQoRhcVH5DHUr",
                        "identifier": "bFQoRhcVH5DHUu",
                        "identifierInPrimarySource": "cp-1",
                        "stableTargetId": "bFQoRhcVH5DHUv",
                    }
                ],
                "total": 7,
            },
        ),
        (
            "?entityType=ExtractedContactPoint",
            {
                "items": [
                    {
                        "$type": "ExtractedContactPoint",
                        "email": ["info@contact-point.one"],
                        "hadPrimarySource": "bFQoRhcVH5DHUr",
                        "identifier": "bFQoRhcVH5DHUu",
                        "identifierInPrimarySource": "cp-1",
                        "stableTargetId": "bFQoRhcVH5DHUv",
                    },
                    {
                        "$type": "ExtractedContactPoint",
                        "email": ["help@contact-point.two"],
                        "hadPrimarySource": "bFQoRhcVH5DHUr",
                        "identifier": "bFQoRhcVH5DHUw",
                        "identifierInPrimarySource": "cp-2",
                        "stableTargetId": "bFQoRhcVH5DHUx",
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
                        "$type": "ExtractedPrimarySource",
                        "alternativeTitle": [],
                        "contact": [],
                        "description": [],
                        "documentation": [],
                        "hadPrimarySource": "00000000000000",
                        "identifier": "bFQoRhcVH5DHUs",
                        "identifierInPrimarySource": "ps-2",
                        "locatedAt": [],
                        "stableTargetId": "bFQoRhcVH5DHUt",
                        "title": [],
                        "unitInCharge": [],
                        "version": "Cool Version v2.13",
                    }
                ],
                "total": 1,
            },
        ),
        (
            "?stableTargetId=bFQoRhcVH5DHUz",
            {
                "items": [
                    {
                        "$type": "ExtractedOrganizationalUnit",
                        "alternativeName": [],
                        "email": [],
                        "hadPrimarySource": "bFQoRhcVH5DHUt",
                        "identifier": "bFQoRhcVH5DHUy",
                        "identifierInPrimarySource": "ou-1",
                        "name": [{"language": "en", "value": "Unit 1"}],
                        "parentUnit": None,
                        "shortName": [],
                        "stableTargetId": "bFQoRhcVH5DHUz",
                        "unitOf": [],
                        "website": [],
                    }
                ],
                "total": 1,
            },
        ),
    ],
    ids=[
        "limit 1",
        "skip 1",
        "entity type contact points",
        "full text search",
        "stable target id filter",
    ],
)
@pytest.mark.usefixtures("load_dummy_data")
@pytest.mark.integration
def test_search_extracted_items(
    client_with_api_key_read_permission: TestClient,
    query_string: str,
    expected: dict[str, Any],
) -> None:
    response = client_with_api_key_read_permission.get(
        f"/v0/extracted-item{query_string}"
    )
    assert response.status_code == 200, response.text
    assert response.json() == expected
