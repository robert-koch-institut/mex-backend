from typing import Any
from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient


def test_search_extracted_items_mocked(
    client_with_api_key_read_permission: TestClient, mocked_graph: MagicMock
) -> None:
    mocked_graph.return_value = [
        {
            "c": 0,
            "l": "ContactPoint",
            "r": [{"key": "hadPrimarySource", "value": ["2222222222222222"]}],
            "n": {
                "stableTargetId": "0000000000000000",
                "identifier": "1111111111111111",
                "identifierInPrimarySource": "test",
                "email": "test@foo.bar",
            },
        }
    ]

    response = client_with_api_key_read_permission.get("/v0/extracted-item")
    assert response.status_code == 200, response.text
    assert response.json() == {
        "items": [
            {
                "$type": "ContactPoint",
                "email": ["test@foo.bar"],
                "hadPrimarySource": "2222222222222222",
                "identifier": "1111111111111111",
                "identifierInPrimarySource": "test",
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
                "total": 7,
                "items": [
                    {
                        "stableTargetId": "aSti000000000001",
                        "abstract": [
                            {"value": "An active activity.", "language": "en"},
                            {"value": "Mumble bumble boo.", "language": None},
                        ],
                        "activityType": [],
                        "alternativeTitle": [],
                        "contact": [
                            "cpSti00000000001",
                            "cpSti00000000002",
                            "ouSti00000000001",
                        ],
                        "documentation": [],
                        "end": [],
                        "externalAssociate": [],
                        "funderOrCommissioner": [],
                        "fundingProgram": [],
                        "involvedPerson": [],
                        "involvedUnit": [],
                        "isPartOfActivity": [],
                        "publication": [],
                        "responsibleUnit": ["ouSti00000000001"],
                        "shortName": [],
                        "start": [],
                        "succeeds": [],
                        "theme": ["https://mex.rki.de/item/theme-3"],
                        "title": [{"value": "Activity 1", "language": "en"}],
                        "website": [
                            {
                                "language": None,
                                "title": "Activity Homepage",
                                "url": "https://activity-1",
                            }
                        ],
                        "identifier": "aId0000000000001",
                        "hadPrimarySource": "psSti00000000001",
                        "identifierInPrimarySource": "a-1",
                        "$type": "Activity",
                    }
                ],
            },
        ),
        (
            "?limit=1&skip=1",
            {
                "items": [
                    {
                        "$type": "ContactPoint",
                        "email": ["info@rki.de"],
                        "hadPrimarySource": "psSti00000000001",
                        "identifier": "cpId000000000001",
                        "identifierInPrimarySource": "cp-1",
                        "stableTargetId": "cpSti00000000001",
                    }
                ],
                "total": 7,
            },
        ),
        (
            "?entityType=ContactPoint",
            {
                "items": [
                    {
                        "$type": "ContactPoint",
                        "email": ["info@rki.de"],
                        "hadPrimarySource": "psSti00000000001",
                        "identifier": "cpId000000000001",
                        "identifierInPrimarySource": "cp-1",
                        "stableTargetId": "cpSti00000000001",
                    },
                    {
                        "$type": "ContactPoint",
                        "email": ["mex@rki.de"],
                        "hadPrimarySource": "psSti00000000001",
                        "identifier": "cpId000000000002",
                        "identifierInPrimarySource": "cp-2",
                        "stableTargetId": "cpSti00000000002",
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
                        "$type": "PrimarySource",
                        "alternativeTitle": [],
                        "contact": [],
                        "description": [],
                        "documentation": [],
                        "hadPrimarySource": "psSti00000000001",
                        "identifier": "psId000000000002",
                        "identifierInPrimarySource": "ps-2",
                        "locatedAt": [],
                        "stableTargetId": "psSti00000000002",
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
            "?stableTargetId=ouSti00000000001",
            {
                "items": [
                    {
                        "$type": "OrganizationalUnit",
                        "alternativeName": [],
                        "email": [],
                        "hadPrimarySource": "psSti00000000002",
                        "identifier": "ouId000000000001",
                        "identifierInPrimarySource": "ou-1",
                        "name": [{"language": "en", "value": "Unit 1"}],
                        "parentUnit": None,
                        "shortName": [],
                        "stableTargetId": "ouSti00000000001",
                        "unitOf": [],
                        "website": [],
                    },
                    {
                        "$type": "OrganizationalUnit",
                        "alternativeName": [],
                        "email": [],
                        "hadPrimarySource": "psSti00000000001",
                        "identifier": "ouId000000000002",
                        "identifierInPrimarySource": "ou-2",
                        "name": [{"language": "en", "value": "Unit 2"}],
                        "parentUnit": None,
                        "shortName": [],
                        "stableTargetId": "ouSti00000000001",
                        "unitOf": [],
                        "website": [],
                    },
                ],
                "total": 2,
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
