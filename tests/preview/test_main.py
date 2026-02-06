from typing import Any

import pytest
from fastapi.testclient import TestClient
from starlette import status


@pytest.mark.parametrize(
    ("stable_target_id", "json_body", "expected"),
    [
        (
            "bFQoRhcVH5DHUH",
            {
                "$type": "ActivityRuleSetRequest",
                "additive": {
                    "$type": "AdditiveActivity",
                    "end": ["2025"],
                    "title": [{"value": "A new beginning", "language": "en"}],
                },
            },
            {
                "$type": "MergedActivity",
                "contact": ["bFQoRhcVH5DHUz", "bFQoRhcVH5DHUB", "bFQoRhcVH5DHUx"],
                "responsibleUnit": ["bFQoRhcVH5DHUx"],
                "title": [
                    {"value": "Aktivität 1", "language": "de"},
                    {"value": "A new beginning", "language": "en"},
                ],
                "abstract": [
                    {"value": "An active activity.", "language": "en"},
                    {"value": "Eng aktiv Aktivitéit.", "language": None},
                ],
                "start": ["2014-08-24"],
                "end": ["2025"],
                "theme": ["https://mex.rki.de/item/theme-11"],
                "website": [
                    {
                        "language": None,
                        "title": "Activity Homepage",
                        "url": "https://activity-1",
                    }
                ],
                "identifier": "bFQoRhcVH5DHUH",
            },
        ),
        (
            "bFQoRhcVH5DHUH",
            {
                "$type": "ActivityRuleSetRequest",
                "subtractive": {
                    "$type": "SubtractiveActivity",
                    "start": ["2014"],
                    "contact": ["bFQoRhcVH5DHUx"],
                    "abstract": [
                        {"value": "Eng aktiv Aktivitéit.", "language": None},
                    ],
                },
            },
            {
                "contact": ["bFQoRhcVH5DHUz", "bFQoRhcVH5DHUB"],
                "responsibleUnit": ["bFQoRhcVH5DHUx"],
                "title": [{"value": "Aktivität 1", "language": "de"}],
                "abstract": [{"value": "An active activity.", "language": "en"}],
                "start": ["2014-08-24"],
                "theme": ["https://mex.rki.de/item/theme-11"],
                "website": [
                    {
                        "language": None,
                        "title": "Activity Homepage",
                        "url": "https://activity-1",
                    }
                ],
                "$type": "MergedActivity",
                "identifier": "bFQoRhcVH5DHUH",
            },
        ),
        (
            "bFQoRhcVH5DHUH",
            {
                "$type": "ActivityRuleSetRequest",
                "preventive": {
                    "$type": "PreventiveActivity",
                    "theme": ["bFQoRhcVH5DHUr"],
                    "title": ["bFQoRhcVH5DHUt"],
                },
            },
            {
                "contact": ["bFQoRhcVH5DHUz", "bFQoRhcVH5DHUB", "bFQoRhcVH5DHUx"],
                "responsibleUnit": ["bFQoRhcVH5DHUx"],
                "title": [{"value": "Aktivität 1", "language": "de"}],
                "abstract": [
                    {"value": "An active activity.", "language": "en"},
                    {"value": "Eng aktiv Aktivitéit.", "language": None},
                ],
                "start": ["2014-08-24"],
                "website": [
                    {
                        "language": None,
                        "title": "Activity Homepage",
                        "url": "https://activity-1",
                    }
                ],
                "$type": "MergedActivity",
                "identifier": "bFQoRhcVH5DHUH",
            },
        ),
        (
            "unknownStableTargetId",
            {
                "$type": "ContactPointRuleSetRequest",
                "additive": {
                    "$type": "AdditiveContactPoint",
                    "email": ["test@test.local"],
                },
                "preventive": {"$type": "PreventiveContactPoint"},
                "subtractive": {"$type": "SubtractiveContactPoint"},
            },
            {
                "$type": "MergedContactPoint",
                "email": ["test@test.local"],
                "identifier": "unknownStableTargetId",
            },
        ),
    ],
    ids=[
        "additive",
        "subtractive",
        "preventive",
        "unknown-id",
    ],
)
@pytest.mark.integration
@pytest.mark.usefixtures("load_dummy_data")
def test_preview(
    stable_target_id: str,
    client_with_api_key_read_permission: TestClient,
    json_body: dict[str, Any],
    expected: dict[str, Any],
) -> None:
    response = client_with_api_key_read_permission.post(
        f"/v0/preview-item/{stable_target_id}", json=json_body
    )
    assert response.status_code == status.HTTP_200_OK, response.text
    cleaned_response = {k: v for k, v in response.json().items() if v}
    assert cleaned_response == expected


@pytest.mark.parametrize(
    ("query_string", "expected"),
    [
        (
            "?limit=1",
            {
                "items": [
                    {
                        "$type": "PreviewPrimarySource",
                        "alternativeTitle": [],
                        "contact": [],
                        "description": [],
                        "documentation": [],
                        "identifier": "00000000000000",
                        "locatedAt": [],
                        "supersededBy": None,
                        "title": [],
                        "unitInCharge": [],
                        "version": None,
                    }
                ],
                "total": 10,
            },
        ),
        (
            "?limit=1&skip=9",
            {
                "items": [
                    {
                        "email": ["info@contact-point.one"],
                        "$type": "PreviewContactPoint",
                        "identifier": "bFQoRhcVH5DHUz",
                        "supersededBy": None,
                    }
                ],
                "total": 10,
            },
        ),
        (
            "?entityType=MergedContactPoint",
            {
                "items": [
                    {
                        "$type": "PreviewContactPoint",
                        "email": ["help@contact-point.two"],
                        "identifier": "bFQoRhcVH5DHUB",
                        "supersededBy": None,
                    },
                    {
                        "$type": "PreviewContactPoint",
                        "email": ["info@contact-point.one"],
                        "identifier": "bFQoRhcVH5DHUz",
                        "supersededBy": None,
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
                        "$type": "PreviewPrimarySource",
                        "alternativeTitle": [],
                        "contact": [],
                        "description": [],
                        "documentation": [],
                        "identifier": "bFQoRhcVH5DHUt",
                        "locatedAt": [],
                        "supersededBy": None,
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
                        "$type": "PreviewOrganizationalUnit",
                        "alternativeName": [],
                        "email": [],
                        "identifier": "bFQoRhcVH5DHUx",
                        "name": [{"language": "en", "value": "Unit 1"}],
                        "parentUnit": None,
                        "shortName": [],
                        "supersededBy": None,
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
                        "$type": "PreviewOrganizationalUnit",
                        "alternativeName": [],
                        "email": [],
                        "identifier": "bFQoRhcVH5DHUF",
                        "name": [
                            {"language": "en", "value": "Unit 1.6"},
                            {"language": "en", "value": "Unit 1.7"},
                        ],
                        "parentUnit": "bFQoRhcVH5DHUx",
                        "shortName": [],
                        "supersededBy": None,
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
            "?hadPrimarySource=bFQoRhcVH5DHUt",  # deprecated
            {
                "items": [
                    {
                        "$type": "PreviewOrganization",
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
                        "supersededBy": None,
                        "viafId": [],
                        "wikidataId": [],
                    },
                    {
                        "$type": "PreviewOrganizationalUnit",
                        "alternativeName": [],
                        "email": [],
                        "identifier": "bFQoRhcVH5DHUF",
                        "name": [
                            {"language": "en", "value": "Unit 1.6"},
                            {"language": "en", "value": "Unit 1.7"},
                        ],
                        "parentUnit": "bFQoRhcVH5DHUx",
                        "shortName": [],
                        "supersededBy": None,
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
                        "$type": "PreviewOrganizationalUnit",
                        "alternativeName": [],
                        "email": [],
                        "identifier": "bFQoRhcVH5DHUx",
                        "name": [{"language": "en", "value": "Unit 1"}],
                        "parentUnit": None,
                        "shortName": [],
                        "supersededBy": None,
                        "unitOf": ["bFQoRhcVH5DHUv"],
                        "website": [],
                    },
                ],
                "total": 3,
            },
        ),
        (
            "?referencedIdentifier=bFQoRhcVH5DHUt&referenceField=hadPrimarySource",
            {
                "items": [
                    {
                        "$type": "PreviewOrganization",
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
                        "supersededBy": None,
                        "viafId": [],
                        "wikidataId": [],
                    },
                    {
                        "$type": "PreviewOrganizationalUnit",
                        "alternativeName": [],
                        "email": [],
                        "identifier": "bFQoRhcVH5DHUF",
                        "name": [
                            {"language": "en", "value": "Unit 1.6"},
                            {"language": "en", "value": "Unit 1.7"},
                        ],
                        "parentUnit": "bFQoRhcVH5DHUx",
                        "shortName": [],
                        "supersededBy": None,
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
                        "$type": "PreviewOrganizationalUnit",
                        "alternativeName": [],
                        "email": [],
                        "identifier": "bFQoRhcVH5DHUx",
                        "name": [{"language": "en", "value": "Unit 1"}],
                        "parentUnit": None,
                        "shortName": [],
                        "supersededBy": None,
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
        "skip 9",
        "entity type contact points",
        "full text search",
        "identifier filter",
        "identifier filter with composite result",
        "had primary source filter",
        "generic id filter",
        "identifier not found",
        "full text not found",
    ],
)
@pytest.mark.usefixtures("load_dummy_data", "load_dummy_rule_set")
@pytest.mark.integration
def test_search_preview_items(
    client_with_api_key_read_permission: TestClient,
    query_string: str,
    expected: dict[str, Any],
) -> None:
    response = client_with_api_key_read_permission.get(
        f"/v0/preview-item{query_string}"
    )
    assert response.status_code == status.HTTP_200_OK, response.text
    assert response.json() == expected


@pytest.mark.integration
def test_search_preview_items_in_graph_bad_request(
    client_with_api_key_read_permission: TestClient,
) -> None:
    response = client_with_api_key_read_permission.get(
        "/v0/preview-item?referenceField=hadPrimarySource"
    )
    assert response.status_code == status.HTTP_400_BAD_REQUEST, response.text
    assert (
        "Must provide referencedIdentifier AND referenceField or neither."
        in response.text
    )
