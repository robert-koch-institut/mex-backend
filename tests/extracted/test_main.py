from typing import Any
from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient


def test_search_extracted_items_mocked(
    client_with_api_key_read_permission: TestClient, mocked_graph: MagicMock
) -> None:
    mocked_graph.return_value = [
        {
            "c": 1,
            "l": "ExtractedContactPoint",
            "r": [{"key": "hadPrimarySource", "value": ["2222222222222222"]}],
            "n": {
                "stableTargetId": "0000000000000000",
                "identifier": "1111111111111111",
                "identifierInPrimarySource": "test",
                "email": "test@foo.bar",
            },
            "i": {
                "stableTargetId": "0000000000000000",
                "hadPrimarySource": "2222222222222222",
                "identifierInPrimarySource": "test",
                "identifier": "1111111111111111",
            },
        },
    ]

    response = client_with_api_key_read_permission.get("/v0/extracted-item")
    assert response.status_code == 200, response.text
    assert response.json() == {
        "items": [
            {
                "$type": "ExtractedContactPoint",
                "email": ["test@foo.bar"],
                "hadPrimarySource": "2222222222222222",
                "identifier": "1111111111111111",
                "identifierInPrimarySource": "test",
                "stableTargetId": "0000000000000000",
            }
        ],
        "total": 1,
    }


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
                        "email": ["info@rki.de"],
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
                        "email": ["info@rki.de"],
                        "hadPrimarySource": "bFQoRhcVH5DHUr",
                        "identifier": "bFQoRhcVH5DHUu",
                        "identifierInPrimarySource": "cp-1",
                        "stableTargetId": "bFQoRhcVH5DHUv",
                    },
                    {
                        "$type": "ExtractedContactPoint",
                        "email": ["mex@rki.de"],
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
                        "title": [
                            {"language": None, "value": "A cool and searchable title"}
                        ],
                        "unitInCharge": [],
                        "version": None,
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
