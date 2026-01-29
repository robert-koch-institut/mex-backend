from typing import Any, cast
from unittest.mock import Mock

import pytest
from fastapi.testclient import TestClient
from starlette import status

from mex.backend.rules.helpers import update_and_get_rule_set
from mex.common.merged.main import create_merged_item
from mex.common.models import (
    AnyExtractedModel,
    ExtractedOrganizationalUnit,
    OrganizationalUnitRuleSetRequest,
    OrganizationalUnitRuleSetResponse,
    SubtractiveOrganizationalUnit,
)
from mex.common.types import Validation
from tests.conftest import MockedGraph


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
                        "supersededBy": None,
                        "title": [],
                        "unitInCharge": [],
                        "version": None,
                    }
                ],
                "total": 9,
            },
        ),
        (
            "?limit=1&skip=8",
            {
                "items": [
                    {
                        "email": ["info@contact-point.one"],
                        "$type": "MergedContactPoint",
                        "identifier": "bFQoRhcVH5DHUz",
                        "supersededBy": None,
                    }
                ],
                "total": 9,
            },
        ),
        (
            "?entityType=MergedContactPoint",
            {
                "items": [
                    {
                        "$type": "MergedContactPoint",
                        "email": ["help@contact-point.two"],
                        "identifier": "bFQoRhcVH5DHUB",
                        "supersededBy": None,
                    },
                    {
                        "$type": "MergedContactPoint",
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
        ),
        (
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
                        "$type": "MergedOrganizationalUnit",
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
                        "$type": "MergedOrganization",
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
                        "$type": "MergedOrganizationalUnit",
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
                        "$type": "MergedOrganizationalUnit",
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
                        "$type": "MergedOrganization",
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
                        "$type": "MergedOrganizationalUnit",
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
                        "$type": "MergedOrganizationalUnit",
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
        "skip 1",
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
    load_dummy_data: dict[str, AnyExtractedModel],
) -> None:
    response = client_with_api_key_read_permission.get(
        "/v0/merged-item?entityType=MergedOrganizationalUnit"
    )
    assert response.status_code == status.HTTP_200_OK, response.text
    assert len(response.json()["items"]) == 2

    # remove the name from org unit 2 to make it invalid
    organizational_unit_2 = cast(
        "ExtractedOrganizationalUnit", load_dummy_data["organizational_unit_2"]
    )
    update_and_get_rule_set(
        stable_target_id=organizational_unit_2.stableTargetId,
        rule_set=OrganizationalUnitRuleSetRequest(
            subtractive=SubtractiveOrganizationalUnit(name=organizational_unit_2.name)
        ),
    )
    response = client_with_api_key_read_permission.get(
        "/v0/merged-item?entityType=MergedOrganizationalUnit"
    )
    assert response.status_code == status.HTTP_200_OK, response.text
    # expect org unit 2 to be filtered out (only unit 1 remains)
    assert response.json()["items"] == [
        {
            "parentUnit": None,
            "name": [{"value": "Unit 1", "language": "en"}],
            "alternativeName": [],
            "email": [],
            "shortName": [],
            "supersededBy": None,
            "unitOf": ["bFQoRhcVH5DHUv"],
            "website": [],
            "$type": "MergedOrganizationalUnit",
            "identifier": "bFQoRhcVH5DHUx",
        }
    ]


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
    load_dummy_data: dict[str, AnyExtractedModel],
) -> None:
    extracted_organization_1 = load_dummy_data["organization_1"]
    extracted_organization_2 = load_dummy_data["organization_2"]
    merged_organization = create_merged_item(
        identifier=extracted_organization_1.stableTargetId,
        extracted_items=[extracted_organization_2, extracted_organization_1],
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
        ("activity_1", ""),
        ("activity_1", "include_rule_set=true"),
        ("organizational_unit_2", "include_rule_set=true"),
        ("standalone_rule_set", "include_rule_set=true"),
    ],
    ids=[
        "item without rule set does not need parameter",
        "item without rule set can have parameter",
        "item with rule set needs parameter",
        "rule-set-only item needs parameter",
    ],
)
@pytest.mark.usefixtures("load_dummy_rule_set")
@pytest.mark.integration
def test_delete_merged_item(
    client_with_api_key_write_permission: TestClient,
    load_dummy_data: dict[str, AnyExtractedModel],
    load_standalone_dummy_rule_set: OrganizationalUnitRuleSetResponse,
    item_name: str,
    url_params: str,
) -> None:
    # Get item for current test
    item = {
        **load_dummy_data,
        "standalone_rule_set": load_standalone_dummy_rule_set,
    }[item_name]

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
        (
            "organizational_unit_2",
            "",
            status.HTTP_412_PRECONDITION_FAILED,
            status.HTTP_200_OK,
        ),
        (
            "organizational_unit_2",
            "include_rule_set=false",
            status.HTTP_412_PRECONDITION_FAILED,
            status.HTTP_200_OK,
        ),
        (
            "contact_point_1",
            "",
            status.HTTP_409_CONFLICT,
            status.HTTP_200_OK,
        ),
        (
            "not_a_real_item",
            "",
            status.HTTP_404_NOT_FOUND,
            status.HTTP_404_NOT_FOUND,
        ),
    ],
    ids=[
        "item with rule set without parameter",
        "item with rule set with explicit parameter false",
        "item with inbound connections",
        "non-existent items not found",
    ],
)
@pytest.mark.usefixtures("load_dummy_rule_set")
@pytest.mark.integration
def test_delete_merged_item_fails(  # noqa: PLR0913
    client_with_api_key_write_permission: TestClient,
    load_dummy_data: dict[str, AnyExtractedModel],
    item_name: str,
    url_params: str,
    status_code_delete: int,
    status_code_after: int,
) -> None:
    # Get item for current test
    item = load_dummy_data.get(item_name, Mock(stableTargetId="notARealIdentifier"))

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
