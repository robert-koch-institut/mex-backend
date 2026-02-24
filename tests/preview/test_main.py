from typing import TYPE_CHECKING, Any

import pytest
from starlette import status

from mex.common.models import MEX_EDITOR_PRIMARY_SOURCE_STABLE_TARGET_ID

if TYPE_CHECKING:  # pragma: no cover
    from fastapi.testclient import TestClient


@pytest.mark.parametrize(
    ("stable_target_id", "json_body", "expected"),
    [
        pytest.param(
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
                "abstract": [
                    {"language": "en", "value": "An active activity."},
                    {"language": None, "value": "Eng aktiv Aktivitéit."},
                ],
                "contact": [
                    "bFQoRhcVH5DHUB",
                    "bFQoRhcVH5DHUD",
                    "bFQoRhcVH5DHUx",
                ],
                "end": ["2025"],
                "identifier": "bFQoRhcVH5DHUH",
                "responsibleUnit": ["bFQoRhcVH5DHUx"],
                "start": ["2014-08-24"],
                "theme": ["https://mex.rki.de/item/theme-11"],
                "title": [
                    {"language": "de", "value": "Aktivität 1"},
                    {"language": "en", "value": "A new beginning"},
                ],
                "website": [
                    {
                        "language": None,
                        "title": "Activity Homepage",
                        "url": "https://activity-1",
                    }
                ],
            },
            id="additive",
        ),
        pytest.param(
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
                "contact": ["bFQoRhcVH5DHUB", "bFQoRhcVH5DHUD"],
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
            id="subtractive",
        ),
        pytest.param(
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
                "contact": ["bFQoRhcVH5DHUB", "bFQoRhcVH5DHUD", "bFQoRhcVH5DHUx"],
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
            id="preventive",
        ),
        pytest.param(
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
            id="unknown-id",
        ),
    ],
)
@pytest.mark.integration
@pytest.mark.usefixtures("loaded_dummy_data")
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
        pytest.param(
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
                "total": 12,
            },
            id="limit-1",
        ),
        pytest.param(
            "?limit=1&skip=9",
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
                        ],
                        "rorId": [],
                        "shortName": [],
                        "supersededBy": None,
                        "viafId": [],
                        "wikidataId": [],
                    }
                ],
                "total": 12,
            },
            id="skip-8",
        ),
        pytest.param(
            "?entityType=MergedContactPoint",
            {
                "items": [
                    {
                        "$type": "PreviewContactPoint",
                        "email": ["info@contact-point.one"],
                        "identifier": "bFQoRhcVH5DHUB",
                        "supersededBy": None,
                    },
                    {
                        "$type": "PreviewContactPoint",
                        "email": ["help@contact-point.two"],
                        "identifier": "bFQoRhcVH5DHUD",
                        "supersededBy": None,
                    },
                ],
                "total": 2,
            },
            id="entity-type-contact-points",
        ),
        pytest.param(
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
            id="full-text-search",
        ),
        pytest.param(
            "?identifier=bFQoRhcVH5DHUx",
            {
                "items": [
                    {
                        "$type": "PreviewOrganizationalUnit",
                        "alternativeName": [],
                        "email": [],
                        "identifier": "bFQoRhcVH5DHUx",
                        "name": [
                            {"language": "en", "value": "Unit 1"},
                        ],
                        "parentUnit": None,
                        "shortName": [],
                        "supersededBy": None,
                        "unitOf": ["bFQoRhcVH5DHUv"],
                        "website": [
                            {"language": None, "title": None, "url": "https://ou-1"}
                        ],
                    }
                ],
                "total": 1,
            },
            id="identifier-filter-extracted-only",
        ),
        pytest.param(
            "?identifier=StandaloneRule",
            {
                "items": [
                    {
                        "$type": "PreviewOrganizationalUnit",
                        "alternativeName": [],
                        "email": ["1.7@rki.de"],
                        "identifier": "StandaloneRule",
                        "name": [{"language": "de", "value": "Abteilung 1.7"}],
                        "parentUnit": "bFQoRhcVH5DHUx",
                        "shortName": [],
                        "supersededBy": None,
                        "unitOf": [],
                        "website": [],
                    }
                ],
                "total": 1,
            },
            id="identifier-filter-rule-set-only",
        ),
        pytest.param(
            "?identifier=bFQoRhcVH5DHUz",
            {
                "items": [
                    {
                        "$type": "PreviewOrganizationalUnit",
                        "alternativeName": [],
                        "email": [],
                        "identifier": "bFQoRhcVH5DHUz",
                        "name": [{"language": "de", "value": "Abteilung 1.6"}],
                        "parentUnit": "bFQoRhcVH5DHUx",
                        "shortName": [],
                        "supersededBy": None,
                        "unitOf": ["bFQoRhcVH5DHUv"],
                        "website": [
                            {
                                "language": None,
                                "title": "Unit Homepage",
                                "url": "https://unit-1-6",
                            }
                        ],
                    }
                ],
                "total": 1,
            },
            id="identifier-filter-extracted-and-ruleset",
        ),
        pytest.param(
            "?referencedIdentifier=bFQoRhcVH5DHUt&referenceField=hadPrimarySource",
            {
                "items": [
                    {
                        "$type": "PreviewOrganization",
                        "alternativeName": [],
                        "geprisId": [],
                        "gndId": [],
                        "identifier": "bFQoRhcVH5DHUF",
                        "isniId": [],
                        "officialName": [
                            {"language": "de", "value": "RKI"},
                            {"language": "en", "value": "Robert Koch Institute"},
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
                        "identifier": "bFQoRhcVH5DHUx",
                        "name": [{"language": "en", "value": "Unit 1"}],
                        "parentUnit": None,
                        "shortName": [],
                        "supersededBy": None,
                        "unitOf": ["bFQoRhcVH5DHUv"],
                        "website": [
                            {"language": None, "title": None, "url": "https://ou-1"}
                        ],
                    },
                    {
                        "$type": "PreviewOrganizationalUnit",
                        "alternativeName": [],
                        "email": [],
                        "identifier": "bFQoRhcVH5DHUz",
                        "name": [{"language": "de", "value": "Abteilung 1.6"}],
                        "parentUnit": "bFQoRhcVH5DHUx",
                        "shortName": [],
                        "supersededBy": None,
                        "unitOf": ["bFQoRhcVH5DHUv"],
                        "website": [
                            {
                                "language": None,
                                "title": "Unit Homepage",
                                "url": "https://unit-1-6",
                            }
                        ],
                    },
                ],
                "total": 3,
            },
            id="referenced-id-filter",
        ),
        pytest.param(
            f"?referencedIdentifier={MEX_EDITOR_PRIMARY_SOURCE_STABLE_TARGET_ID}&referenceField=hadPrimarySource",
            {
                "items": [
                    {
                        "$type": "PreviewOrganizationalUnit",
                        "alternativeName": [],
                        "email": ["1.7@rki.de"],
                        "identifier": "StandaloneRule",
                        "name": [{"language": "de", "value": "Abteilung 1.7"}],
                        "parentUnit": "bFQoRhcVH5DHUx",
                        "shortName": [],
                        "supersededBy": None,
                        "unitOf": [],
                        "website": [],
                    },
                    {
                        "$type": "PreviewOrganizationalUnit",
                        "alternativeName": [],
                        "email": [],
                        "identifier": "bFQoRhcVH5DHUz",
                        "name": [{"language": "de", "value": "Abteilung 1.6"}],
                        "parentUnit": "bFQoRhcVH5DHUx",
                        "shortName": [],
                        "supersededBy": None,
                        "unitOf": ["bFQoRhcVH5DHUv"],
                        "website": [
                            {
                                "language": None,
                                "title": "Unit Homepage",
                                "url": "https://unit-1-6",
                            }
                        ],
                    },
                ],
                "total": 2,
            },
            id="had-primary-source-mex-editor-filter",
        ),
        pytest.param(
            "?identifier=thisIdDoesNotExist",
            {"items": [], "total": 0},
            id="identifier-not-found",
        ),
        pytest.param(
            "?q=queryNotFound",
            {"items": [], "total": 0},
            id="full-text-not-found",
        ),
    ],
)
@pytest.mark.usefixtures("loaded_dummy_data")
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
