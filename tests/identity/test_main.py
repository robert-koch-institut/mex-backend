from typing import Any

import pytest
from fastapi.testclient import TestClient

from mex.common.models import (
    MEX_PRIMARY_SOURCE_IDENTIFIER,
    MEX_PRIMARY_SOURCE_IDENTIFIER_IN_PRIMARY_SOURCE,
    MEX_PRIMARY_SOURCE_STABLE_TARGET_ID,
)
from tests.conftest import MockedGraph


@pytest.mark.parametrize(
    ("mocked_return", "post_body", "expected"),
    [
        (
            [],
            {
                "hadPrimarySource": "psSti00000000001",
                "identifierInPrimarySource": "new-item",
            },
            {
                "hadPrimarySource": "psSti00000000001",
                "identifier": "bFQoRhcVH5DHUq",
                "identifierInPrimarySource": "new-item",
                "stableTargetId": "bFQoRhcVH5DHUr",
            },
        ),
        (
            [
                {
                    "hadPrimarySource": "psSti00000000001",
                    "identifier": "cpId000000000002",
                    "identifierInPrimarySource": "cp-2",
                    "stableTargetId": "cpSti00000000002",
                }
            ],
            {
                "hadPrimarySource": "psSti00000000001",
                "identifierInPrimarySource": "cp-2",
            },
            {
                "hadPrimarySource": "psSti00000000001",
                "identifier": "cpId000000000002",
                "identifierInPrimarySource": "cp-2",
                "stableTargetId": "cpSti00000000002",
            },
        ),
    ],
    ids=["new item", "existing contact point"],
)
def test_assign_identity_mocked(
    client_with_api_key_write_permission: TestClient,
    mocked_graph: MockedGraph,
    mocked_return: list[dict[str, str]],
    post_body: dict[str, str],
    expected: dict[str, Any],
) -> None:
    mocked_graph.return_value = mocked_return
    response = client_with_api_key_write_permission.post("/v0/identity", json=post_body)
    assert response.status_code == 200, response.text
    assert response.json() == expected


def test_assign_identity_inconsistency_mocked(
    client_with_api_key_write_permission: TestClient,
    mocked_graph: MockedGraph,
) -> None:
    mocked_graph.return_value = [
        {
            "hadPrimarySource": "psSti00000000001",
            "identifier": "cpId000000000002",
            "identifierInPrimarySource": "cp-2",
            "stableTargetId": "cpSti00000000002",
        },
        {
            "hadPrimarySource": "psSti00000000001",
            "identifier": "cpId000000000098",
            "identifierInPrimarySource": "cp-2",
            "stableTargetId": "cpSti00000000099",
        },
    ]
    response = client_with_api_key_write_permission.post(
        "/v0/identity",
        json={
            "hadPrimarySource": "psSti00000000001",
            "identifierInPrimarySource": "cp-2",
        },
    )
    assert response.status_code == 500, response.text
    assert "MultipleResultsFoundError" in response.text


@pytest.mark.parametrize(
    ("post_body", "expected"),
    [
        (
            {
                "hadPrimarySource": "bFQoRhcVH5DHUr",
                "identifierInPrimarySource": "new-item",
            },
            {
                "hadPrimarySource": "bFQoRhcVH5DHUr",
                "identifier": "bFQoRhcVH5DHUC",
                "identifierInPrimarySource": "new-item",
                "stableTargetId": "bFQoRhcVH5DHUD",
            },
        ),
        (
            {
                "hadPrimarySource": "bFQoRhcVH5DHUr",
                "identifierInPrimarySource": "cp-2",
            },
            {
                "hadPrimarySource": "bFQoRhcVH5DHUr",
                "identifier": "bFQoRhcVH5DHUw",
                "identifierInPrimarySource": "cp-2",
                "stableTargetId": "bFQoRhcVH5DHUD",
            },
        ),
        (
            {
                "hadPrimarySource": MEX_PRIMARY_SOURCE_STABLE_TARGET_ID,
                "identifierInPrimarySource": MEX_PRIMARY_SOURCE_IDENTIFIER_IN_PRIMARY_SOURCE,
            },
            {
                "hadPrimarySource": MEX_PRIMARY_SOURCE_STABLE_TARGET_ID,
                "identifier": MEX_PRIMARY_SOURCE_IDENTIFIER,
                "identifierInPrimarySource": MEX_PRIMARY_SOURCE_IDENTIFIER_IN_PRIMARY_SOURCE,
                "stableTargetId": MEX_PRIMARY_SOURCE_STABLE_TARGET_ID,
            },
        ),
    ],
    ids=["new item", "existing contact point", "mex primary source"],
)
@pytest.mark.usefixtures("load_dummy_data")
@pytest.mark.integration
def test_assign_identity(
    client_with_api_key_write_permission: TestClient,
    post_body: dict[str, str],
    expected: dict[str, Any],
) -> None:
    response = client_with_api_key_write_permission.post("/v0/identity", json=post_body)
    assert response.status_code == 200, response.text
    assert response.json() == expected


@pytest.mark.parametrize(
    ("mocked_return", "query_string", "expected"),
    [
        ([], "?stableTargetId=thisDoesNotExist", {"items": [], "total": 0}),
        (
            [
                {
                    "hadPrimarySource": "28282828282828",
                    "identifier": "7878787878787878777",
                    "identifierInPrimarySource": "one",
                    "stableTargetId": "949494949494949494",
                }
            ],
            "?hadPrimarySource=28282828282828&identifierInPrimarySource=one",
            {
                "items": [
                    {
                        "hadPrimarySource": "28282828282828",
                        "identifier": "7878787878787878777",
                        "identifierInPrimarySource": "one",
                        "stableTargetId": "949494949494949494",
                    },
                ],
                "total": 1,
            },
        ),
        (
            [
                {
                    "hadPrimarySource": "28282828282828",
                    "identifier": "62626262626266262",
                    "identifierInPrimarySource": "two",
                    "stableTargetId": "949494949494949494",
                },
                {
                    "hadPrimarySource": "39393939393939",
                    "identifier": "7878787878787878777",
                    "identifierInPrimarySource": "duo",
                    "stableTargetId": "949494949494949494",
                },
            ],
            "?stableTargetId=949494949494949494",
            {
                "items": [
                    {
                        "hadPrimarySource": "28282828282828",
                        "identifier": "62626262626266262",
                        "identifierInPrimarySource": "two",
                        "stableTargetId": "949494949494949494",
                    },
                    {
                        "hadPrimarySource": "39393939393939",
                        "identifier": "7878787878787878777",
                        "identifierInPrimarySource": "duo",
                        "stableTargetId": "949494949494949494",
                    },
                ],
                "total": 2,
            },
        ),
    ],
    ids=["nothing found", "one item", "two items"],
)
def test_fetch_identities_mocked(
    client_with_api_key_write_permission: TestClient,
    mocked_graph: MockedGraph,
    mocked_return: list[dict[str, str]],
    query_string: str,
    expected: dict[str, Any],
) -> None:
    mocked_graph.return_value = mocked_return
    response = client_with_api_key_write_permission.get(f"/v0/identity{query_string}")
    assert response.status_code == 200, response.text
    assert response.json() == expected


@pytest.mark.parametrize(
    ("query_string", "expected"),
    [
        ("?stableTargetId=thisDoesNotExist", {"items": [], "total": 0}),
        (
            "?hadPrimarySource=00000000000000&identifierInPrimarySource=ps-1",
            {
                "items": [
                    {
                        "identifier": "bFQoRhcVH5DHUr",
                        "hadPrimarySource": "00000000000000",
                        "identifierInPrimarySource": "ps-1",
                        "stableTargetId": "bFQoRhcVH5DHUr",
                    }
                ],
                "total": 1,
            },
        ),
        (
            "?stableTargetId=bFQoRhcVH5DHUy",
            {
                "items": [
                    {
                        "identifier": "bFQoRhcVH5DHUz",
                        "hadPrimarySource": "bFQoRhcVH5DHUs",
                        "identifierInPrimarySource": "ou-1",
                        "stableTargetId": "bFQoRhcVH5DHUy",
                    }
                ],
                "total": 1,
            },
        ),
    ],
    ids=[
        "nothing found",
        "by hadPrimarySource and identifierInPrimarySource",
        "by stableTargetId",
    ],
)
@pytest.mark.usefixtures("load_dummy_data")
@pytest.mark.integration
def test_fetch_identities(
    client_with_api_key_write_permission: TestClient,
    query_string: str,
    expected: dict[str, Any],
) -> None:
    response = client_with_api_key_write_permission.get(f"/v0/identity{query_string}")
    assert response.status_code == 200, response.text
    assert response.json() == expected
