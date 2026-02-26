from typing import TYPE_CHECKING, Any

import pytest
from fastapi.encoders import jsonable_encoder
from starlette import status

from mex.common.models import (
    MEX_EDITOR_PRIMARY_SOURCE_STABLE_TARGET_ID,
    ExtractedOrganizationalUnit,
)

if TYPE_CHECKING:  # pragma: no cover
    from fastapi.testclient import TestClient

    from tests.conftest import DummyData, MockedGraph


@pytest.mark.usefixtures("mocked_valkey")
def test_search_extracted_items_mocked(
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
        pytest.param(
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
                "total": 11,
            },
            id="limit-1",
        ),
        pytest.param(
            "?limit=1&skip=10",
            {
                "items": [
                    {
                        "$type": "ExtractedOrganizationalUnit",
                        "alternativeName": [],
                        "email": [],
                        "hadPrimarySource": "bFQoRhcVH5DHUt",
                        "identifier": "bFQoRhcVH5DHUy",
                        "identifierInPrimarySource": "ou-1.6",
                        "name": [{"language": "en", "value": "Unit 1.6"}],
                        "parentUnit": None,
                        "shortName": [],
                        "stableTargetId": "bFQoRhcVH5DHUz",
                        "unitOf": ["bFQoRhcVH5DHUv"],
                        "website": [],
                    }
                ],
                "total": 11,
            },
            id="skip-10",
        ),
        pytest.param(
            "?entityType=ExtractedContactPoint",
            {
                "items": [
                    {
                        "$type": "ExtractedContactPoint",
                        "email": ["info@contact-point.one"],
                        "hadPrimarySource": "bFQoRhcVH5DHUr",
                        "identifier": "bFQoRhcVH5DHUA",
                        "identifierInPrimarySource": "cp-1",
                        "stableTargetId": "bFQoRhcVH5DHUB",
                    },
                    {
                        "$type": "ExtractedContactPoint",
                        "email": ["help@contact-point.two"],
                        "hadPrimarySource": "bFQoRhcVH5DHUr",
                        "identifier": "bFQoRhcVH5DHUC",
                        "identifierInPrimarySource": "cp-2",
                        "stableTargetId": "bFQoRhcVH5DHUD",
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
            id="full-text-search",
        ),
        pytest.param(
            "?stableTargetId=bFQoRhcVH5DHUx",
            {
                "items": [
                    {
                        "$type": "ExtractedOrganizationalUnit",
                        "alternativeName": [],
                        "email": [],
                        "hadPrimarySource": "bFQoRhcVH5DHUt",
                        "identifier": "bFQoRhcVH5DHUw",
                        "identifierInPrimarySource": "ou-1",
                        "name": [{"value": "Unit 1", "language": "en"}],
                        "parentUnit": None,
                        "shortName": [],
                        "stableTargetId": "bFQoRhcVH5DHUx",
                        "unitOf": ["bFQoRhcVH5DHUv"],
                        "website": [
                            {"language": None, "title": None, "url": "https://ou-1"}
                        ],
                    }
                ],
                "total": 1,
            },
            id="stable-target-id-filter",
        ),
        pytest.param(
            f"?referencedIdentifier={MEX_EDITOR_PRIMARY_SOURCE_STABLE_TARGET_ID}&referenceField=hadPrimarySource",
            {"items": [], "total": 0},
            id="had-primary-source-mex-editor-filter",
        ),
        pytest.param(
            "?stableTargetId=thisIdDoesNotExist",
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


@pytest.mark.integration
def test_search_extracted_items_in_graph_bad_request(
    client_with_api_key_read_permission: TestClient,
) -> None:
    response = client_with_api_key_read_permission.get(
        "/v0/extracted-item?referenceField=hadPrimarySource"
    )
    assert response.status_code == status.HTTP_400_BAD_REQUEST, response.text
    assert (
        "Must provide referencedIdentifier AND referenceField or neither."
        in response.text
    )


@pytest.mark.integration
def test_search_extracted_items_invalid_reference_field_filter(
    client_with_api_key_read_permission: TestClient,
) -> None:
    response = client_with_api_key_read_permission.get(
        "/v0/extracted-item?referenceField=description"
    )
    assert response.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT, response.text
    assert (
        "Input should be 'accessPlatform', 'accessService', 'affiliation'"  # ...
        in response.text
    )


@pytest.mark.integration
def test_get_extracted_item(
    client_with_api_key_read_permission: TestClient,
    loaded_dummy_data: DummyData,
) -> None:
    organization_1 = loaded_dummy_data["organization_1"]
    response = client_with_api_key_read_permission.get(
        f"/v0/extracted-item/{organization_1.identifier}"
    )
    assert response.status_code == status.HTTP_200_OK, response.text
    assert response.json() == organization_1.model_dump(by_alias=True)


@pytest.mark.integration
def test_get_extracted_item_not_found(
    client_with_api_key_read_permission: TestClient,
) -> None:
    response = client_with_api_key_read_permission.get(
        "/v0/extracted-item/notARealIdentifier"
    )
    assert response.status_code == status.HTTP_404_NOT_FOUND, response.text
