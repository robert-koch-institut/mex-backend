from typing import Any
from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient


def test_search_merged_items_mocked(
    client_with_api_key_read_permission: TestClient, mocked_graph: MagicMock
) -> None:
    mocked_graph.return_value = [
        {
            "c": 0,
            "l": "ExtractedContactPoint",  # stopgap mx-1382 (search for MergedContactPoint instead)
            "r": [{"key": "hadPrimarySource", "value": ["2222222222222222"]}],
            "n": {
                "stableTargetId": "0000000000000000",
                "identifier": "1111111111111111",
                "identifierInPrimarySource": "test",
                "email": "test@foo.bar",
            },
            "i": {
                "stableTargetId": "0000000000000000",
                "identifier": "1111111111111111",
                "identifierInPrimarySource": "test",
                "hadPrimarySource": "2222222222222222",
            },
        }
    ]

    response = client_with_api_key_read_permission.get("/v0/merged-item")
    assert response.status_code == 200, response.text
    assert response.json() == {
        "items": [
            {
                "$type": "MergedContactPoint",
                "email": ["test@foo.bar"],
                "identifier": "1111111111111111",
                "stableTargetId": "0000000000000000",
            }
        ],
        "total": 0,
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
                        "$type": "MergedContactPoint",
                        "email": ["info@rki.de"],
                        "identifier": "bFQoRhcVH5DHUu",
                        "stableTargetId": "bFQoRhcVH5DHUv",
                    }
                ],
                "total": 7,
            },
        ),
        (
            "?entityType=MergedContactPoint",
            {
                "items": [
                    {
                        "$type": "MergedContactPoint",
                        "email": ["info@rki.de"],
                        "identifier": "bFQoRhcVH5DHUu",
                        "stableTargetId": "bFQoRhcVH5DHUv",
                    },
                    {
                        "$type": "MergedContactPoint",
                        "email": ["mex@rki.de"],
                        "identifier": "bFQoRhcVH5DHUw",
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
                        "$type": "MergedPrimarySource",
                        "alternativeTitle": [],
                        "contact": [],
                        "description": [],
                        "documentation": [],
                        "identifier": "bFQoRhcVH5DHUs",
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
                        "$type": "MergedOrganizationalUnit",
                        "alternativeName": [],
                        "email": [],
                        "identifier": "bFQoRhcVH5DHUy",
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
def test_search_merged_items(
    client_with_api_key_read_permission: TestClient,
    query_string: str,
    expected: dict[str, Any],
) -> None:
    response = client_with_api_key_read_permission.get(f"/v0/merged-item{query_string}")
    assert response.status_code == 200, response.text
    assert response.json() == expected
