from typing import TYPE_CHECKING, Any

import pytest
from starlette import status

from mex.backend.rules.helpers import update_and_get_rule_set
from mex.common.models import (
    MEX_EDITOR_PRIMARY_SOURCE_STABLE_TARGET_ID,
    ExtractedOrganizationalUnit,
    OrganizationalUnitRuleSetRequest,
    SubtractiveOrganizationalUnit,
    WorkflowOrganizationalUnit,
)

if TYPE_CHECKING:  # pragma: no cover
    from fastapi.testclient import TestClient

    from tests.conftest import DummyData, MockedGraph


@pytest.mark.usefixtures("mocked_valkey")
def test_search_publishable_merged_items_mocked(
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

    response = client_with_api_key_read_permission.get(
        "/v0/publishable-merged-item", params={"publishingTarget": "invenio"}
    )
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
                "supersededBy": None,
                "unitOf": [],
                "website": [],
            }
        ],
        "total": 14,
    }


@pytest.mark.parametrize(
    ("query_string", "expected"),
    [
        pytest.param(
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
                        "$type": "MergedOrganization",
                        "alternativeName": [],
                        "geprisId": [],
                        "gndId": [],
                        "identifier": "bFQoRhcVH5DHUv",
                        "isniId": [],
                        "officialName": [{"language": "de", "value": "RKI"}],
                        "rorId": [],
                        "shortName": [],
                        "supersededBy": None,
                        "viafId": [],
                        "wikidataId": [],
                    }
                ],
                "total": 12,
            },
            id="skip-9",
        ),
        pytest.param(
            "?entityType=MergedContactPoint",
            {
                "items": [
                    {
                        "$type": "MergedContactPoint",
                        "email": ["info@contact-point.one"],
                        "identifier": "bFQoRhcVH5DHUB",
                        "supersededBy": None,
                    },
                    {
                        "$type": "MergedContactPoint",
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
                        "$type": "MergedPrimarySource",
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
                        "$type": "MergedOrganizationalUnit",
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
                        "$type": "MergedOrganizationalUnit",
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
                        "$type": "MergedOrganizationalUnit",
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
                        "$type": "MergedOrganization",
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
                        "$type": "MergedOrganizationalUnit",
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
                        "$type": "MergedOrganizationalUnit",
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
                        "$type": "MergedOrganizationalUnit",
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
                        "$type": "MergedOrganizationalUnit",
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
def test_search_publishable_merged_items(
    client_with_api_key_read_permission: TestClient,
    query_string: str,
    expected: dict[str, Any],
) -> None:
    response = client_with_api_key_read_permission.get(
        f"/v0/publishable-merged-item{query_string}&publishingTarget=invenio"
    )
    assert response.status_code == status.HTTP_200_OK, response.text
    assert response.json() == expected


@pytest.mark.integration
def test_search_publishable_merged_items_skip_on_validation_error(
    client_with_api_key_read_permission: TestClient,
    loaded_dummy_data: DummyData,
) -> None:
    response = client_with_api_key_read_permission.get(
        "/v0/publishable-merged-item?entityType=MergedOrganizationalUnit&publishingTarget=invenio",
    )
    assert response.status_code == status.HTTP_200_OK, response.text
    assert len(response.json()["items"]) == 3

    # remove the name from org unit 2 to make it invalid
    unit_2 = loaded_dummy_data["unit_2"]
    update_and_get_rule_set(
        stable_target_id=unit_2.stableTargetId,
        rule_set=OrganizationalUnitRuleSetRequest(
            subtractive=SubtractiveOrganizationalUnit(name=unit_2.name)
        ),
    )
    response = client_with_api_key_read_permission.get(
        "/v0/publishable-merged-item?entityType=MergedOrganizationalUnit&publishingTarget=invenio"
    )
    assert response.status_code == status.HTTP_200_OK, response.text
    # expect org unit 2 to be filtered out (only unit 1 and 3 remain)
    assert response.json() == {
        "items": [
            {
                "$type": "MergedOrganizationalUnit",
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
                "$type": "MergedOrganizationalUnit",
                "alternativeName": [],
                "email": [],
                "identifier": "bFQoRhcVH5DHUx",
                "name": [{"language": "en", "value": "Unit 1"}],
                "parentUnit": None,
                "shortName": [],
                "supersededBy": None,
                "unitOf": ["bFQoRhcVH5DHUv"],
                "website": [{"language": None, "title": None, "url": "https://ou-1"}],
            },
        ],
        "total": 3,  # the total still contains the filtered-out items :/
    }


@pytest.mark.integration
def test_search_publishable_merged_items_in_graph_bad_request(
    client_with_api_key_read_permission: TestClient,
) -> None:
    response = client_with_api_key_read_permission.get(
        "/v0/publishable-merged-item?referenceField=hadPrimarySource&publishingTarget=invenio"
    )
    assert response.status_code == status.HTTP_400_BAD_REQUEST, response.text
    assert (
        "Must provide referencedIdentifier AND referenceField or neither."
        in response.text
    )


@pytest.mark.integration
def test_search_publishable_merged_items_in_graph_skip_on_forbidden_publishing_target(
    client_with_api_key_read_permission: TestClient,
    loaded_dummy_data: DummyData,
) -> None:
    unit_2 = loaded_dummy_data["unit_2"]
    update_and_get_rule_set(
        stable_target_id=unit_2.stableTargetId,
        rule_set=OrganizationalUnitRuleSetRequest(
            workflow=WorkflowOrganizationalUnit(forbiddenPublishingTarget="invenio")
        ),
    )
    response = client_with_api_key_read_permission.get(
        "/v0/publishable-merged-item",
        params={
            "entityType": "MergedOrganizationalUnit",
            "identifier": unit_2.stableTargetId,
            "publishingTarget": "invenio",
        },
    )
    assert response.status_code == status.HTTP_200_OK, response.text
    assert response.json() == {"items": [], "total": 1}
