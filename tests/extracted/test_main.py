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
                        "identifier": "00000000000001",
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
                        "hadPrimarySource": "gGdOIbDIHRt35He616Fv5q",
                        "identifierInPrimarySource": "cp-1",
                        "email": ["info@contact-point.one"],
                        "$type": "ExtractedContactPoint",
                        "identifier": "gs6yL8KJoXRos9l2ydYFfx",
                        "stableTargetId": "wEvxYRPlmGVQCbZx9GAbn",
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
                        "hadPrimarySource": "gGdOIbDIHRt35He616Fv5q",
                        "identifier": "gs6yL8KJoXRos9l2ydYFfx",
                        "identifierInPrimarySource": "cp-1",
                        "stableTargetId": "wEvxYRPlmGVQCbZx9GAbn",
                    },
                    {
                        "$type": "ExtractedContactPoint",
                        "email": ["help@contact-point.two"],
                        "hadPrimarySource": "gGdOIbDIHRt35He616Fv5q",
                        "identifier": "vQHKlAQWWraW9NPoB5Ewq",
                        "identifierInPrimarySource": "cp-2",
                        "stableTargetId": "g32qzYNVH1Ez7JTEk3fvLF",
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
                        "identifier": "d0MGZryflsy7PbsBF3ZGXO",
                        "identifierInPrimarySource": "ps-2",
                        "locatedAt": [],
                        "stableTargetId": "bbTqJnQc3TA8dBJmLMBimb",
                        "title": [],
                        "unitInCharge": [],
                        "version": "Cool Version v2.13",
                    }
                ],
                "total": 1,
            },
        ),
        (
            "?stableTargetId=cWWm02l1c6cucKjIhkFqY4",
            {
                "items": [
                    {
                        "$type": "ExtractedOrganizationalUnit",
                        "alternativeName": [],
                        "email": [],
                        "hadPrimarySource": "bbTqJnQc3TA8dBJmLMBimb",
                        "identifier": "gIyDlXYbq0JwItPRU0NcFN",
                        "identifierInPrimarySource": "ou-1",
                        "name": [{"language": "en", "value": "Unit 1"}],
                        "parentUnit": None,
                        "shortName": [],
                        "stableTargetId": "cWWm02l1c6cucKjIhkFqY4",
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
