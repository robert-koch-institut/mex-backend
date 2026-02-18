from typing import Any
from unittest.mock import Mock

import pytest
from fastapi.testclient import TestClient
from starlette import status

from mex.backend.graph.models import MEX_EDITOR_PRIMARY_SOURCE_STABLE_TARGET_ID
from mex.backend.rules.helpers import update_and_get_rule_set
from mex.common.merged.main import create_merged_item
from mex.common.models import (
    ExtractedOrganizationalUnit,
    OrganizationalUnitRuleSetRequest,
    SubtractiveOrganizationalUnit,
)
from mex.common.types import Validation
from tests.conftest import DummyData, DummyDataName, MockedGraph


@pytest.mark.usefixtures("mocked_valkey")
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
    loaded_dummy_data: DummyData,
) -> None:
    response = client_with_api_key_read_permission.get(
        "/v0/merged-item?entityType=MergedOrganizationalUnit"
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
        "/v0/merged-item?entityType=MergedOrganizationalUnit"
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
def test_search_merged_items_in_graph_bad_request(
    client_with_api_key_read_permission: TestClient,
) -> None:
    response = client_with_api_key_read_permission.get(
        "/v0/merged-item?referenceField=hadPrimarySource"
    )
    assert response.status_code == status.HTTP_400_BAD_REQUEST, response.text
    assert (
        "Must provide referencedIdentifier AND referenceField or neither."
        in response.text
    )


@pytest.mark.integration
def test_get_merged_item(
    client_with_api_key_read_permission: TestClient,
    loaded_dummy_data: DummyData,
) -> None:
    extracted_organization_1 = loaded_dummy_data["organization_1"]

    merged_organization = create_merged_item(
        identifier=extracted_organization_1.stableTargetId,
        extracted_items=[extracted_organization_1],
        rule_set=None,
        validation=Validation.STRICT,
    )
    response = client_with_api_key_read_permission.get(
        f"/v0/merged-item/{merged_organization.identifier}"
    )
    assert response.status_code == status.HTTP_200_OK, response.text
    assert response.json() == merged_organization.model_dump(by_alias=True, mode="json")


@pytest.mark.integration
def test_get_merged_item_not_found(
    client_with_api_key_read_permission: TestClient,
) -> None:
    response = client_with_api_key_read_permission.get(
        "/v0/merged-item/notARealIdentifier"
    )
    assert response.status_code == status.HTTP_404_NOT_FOUND, response.text


@pytest.mark.parametrize(
    ("item_name", "url_params"),
    [
        pytest.param(
            "activity_1",
            "",
            id="no-rule-set-no-param",
        ),
        pytest.param(
            "activity_1",
            "include_rule_set=true",
            id="no-rule-set-with-param",
        ),
        pytest.param(
            "unit_2",
            "include_rule_set=true",
            id="with-rule-set-needs-param",
        ),
        pytest.param(
            "unit_3_standalone_rule_set",
            "include_rule_set=true",
            id="rule-set-only-needs-param",
        ),
    ],
)
@pytest.mark.integration
def test_delete_merged_item(
    client_with_api_key_write_permission: TestClient,
    loaded_dummy_data: DummyData,
    item_name: DummyDataName,
    url_params: str,
) -> None:
    # Get item for current test
    item = loaded_dummy_data[item_name]

    # Attempt to delete the item
    response = client_with_api_key_write_permission.delete(
        f"/v0/merged-item/{item.stableTargetId}?{url_params}"
    )
    assert response.status_code == status.HTTP_204_NO_CONTENT, response.text
    assert response.content == b""

    # Verify item is deleted
    get_response = client_with_api_key_write_permission.get(
        f"/v0/merged-item/{item.stableTargetId}"
    )
    assert get_response.status_code == status.HTTP_404_NOT_FOUND, response.text


@pytest.mark.parametrize(
    (
        "item_name",
        "url_params",
        "status_code_delete",
        "status_code_after",
    ),
    [
        pytest.param(
            "unit_2",
            "",
            status.HTTP_412_PRECONDITION_FAILED,
            status.HTTP_200_OK,
            id="rule-set-without-param",
        ),
        pytest.param(
            "unit_2",
            "include_rule_set=false",
            status.HTTP_412_PRECONDITION_FAILED,
            status.HTTP_200_OK,
            id="rule-set-explicit-false",
        ),
        pytest.param(
            "contact_point_1",
            "",
            status.HTTP_409_CONFLICT,
            status.HTTP_200_OK,
            id="inbound-connections",
        ),
        pytest.param(
            "not_a_real_item",
            "",
            status.HTTP_404_NOT_FOUND,
            status.HTTP_404_NOT_FOUND,
            id="not-found",
        ),
    ],
)
@pytest.mark.integration
def test_delete_merged_item_fails(  # noqa: PLR0913
    client_with_api_key_write_permission: TestClient,
    loaded_dummy_data: DummyData,
    item_name: DummyDataName,
    url_params: str,
    status_code_delete: int,
    status_code_after: int,
) -> None:
    # Get item for current test
    try:
        item = loaded_dummy_data[item_name]
    except KeyError:
        item = Mock(stableTargetId="notARealIdentifier")

    # Should fail when there are rules, but `include_rule_set` is not set to `true`
    response = client_with_api_key_write_permission.delete(
        f"/v0/merged-item/{item.stableTargetId}?{url_params}"
    )
    assert response.status_code == status_code_delete, response.text

    # Item should still exist (deletion failed)
    get_response = client_with_api_key_write_permission.get(
        f"/v0/merged-item/{item.stableTargetId}"
    )
    assert get_response.status_code == status_code_after, response.text
