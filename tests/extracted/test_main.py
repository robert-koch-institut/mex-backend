from typing import Any

import pytest
from fastapi.encoders import jsonable_encoder
from fastapi.testclient import TestClient
from starlette import status

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
    assert response.status_code == status.HTTP_200_OK, response.text
    assert response.json() == {"items": [jsonable_encoder(unit)], "total": 14}


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
                        "identifier": "00000000000001",
                        "identifierInPrimarySource": "mex",
                        "locatedAt": [],
                        "stableTargetId": "00000000000000",
                        "title": [],
                        "unitInCharge": [],
                        "version": None,
                    }
                ],
                "total": 8,
            },
        ),
        (
            "?limit=1&skip=6",
            {
                "items": [
                    {
                        "$type": "ExtractedContactPoint",
                        "email": ["info@contact-point.one"],
                        "hadPrimarySource": "bFQoRhcVH5DHUr",
                        "identifier": "bFQoRhcVH5DHUw",
                        "identifierInPrimarySource": "cp-1",
                        "stableTargetId": "bFQoRhcVH5DHUx",
                    }
                ],
                "total": 8,
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
                        "identifier": "bFQoRhcVH5DHUw",
                        "identifierInPrimarySource": "cp-1",
                        "stableTargetId": "bFQoRhcVH5DHUx",
                    },
                    {
                        "$type": "ExtractedContactPoint",
                        "email": ["help@contact-point.two"],
                        "hadPrimarySource": "bFQoRhcVH5DHUr",
                        "identifier": "bFQoRhcVH5DHUy",
                        "identifierInPrimarySource": "cp-2",
                        "stableTargetId": "bFQoRhcVH5DHUz",
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
            "?stableTargetId=bFQoRhcVH5DHUv",
            {
                "items": [
                    {
                        "$type": "ExtractedOrganizationalUnit",
                        "alternativeName": [],
                        "email": [],
                        "hadPrimarySource": "bFQoRhcVH5DHUt",
                        "identifier": "bFQoRhcVH5DHUu",
                        "identifierInPrimarySource": "ou-1",
                        "name": [{"language": "en", "value": "Unit 1"}],
                        "parentUnit": None,
                        "shortName": [],
                        "stableTargetId": "bFQoRhcVH5DHUv",
                        "unitOf": [],
                        "website": [],
                    }
                ],
                "total": 1,
            },
        ),
        ("?stableTargetId=thisIdDoesNotExist", {"items": [], "total": 0}),
        ("?q=queryNotFound", {"items": [], "total": 0}),
    ],
    ids=[
        "limit 1",
        "skip 1",
        "entity type contact points",
        "full text search",
        "stable target id filter",
        "identifier not found",
        "full text not found",
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
    assert response.status_code == status.HTTP_200_OK, response.text
    assert response.json() == expected
