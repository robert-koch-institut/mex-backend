import json
from typing import Any

import pytest
from fastapi.testclient import TestClient

from mex.backend.graph.connector import GraphConnector


def get_graph() -> list[dict[str, Any]]:
    connector = GraphConnector.get()
    graph = connector.commit(
        """
CALL {
    MATCH (n)
    RETURN collect(n{
        .*, label: head(labels(n))
    }) as nodes
}
CALL {
    MATCH ()-[r]->()
    RETURN collect({
        label: type(r), position: r.position,
        start: coalesce(startNode(r).identifier, head(labels(startNode(r)))),
        end: coalesce(endNode(r).identifier, head(labels(endNode(r))))
    }) as relations
}
RETURN nodes, relations;
"""
    ).one()
    return sorted(
        graph["nodes"] + graph["relations"],
        key=lambda i: json.dumps(i, sort_keys=True),
    )


@pytest.mark.integration
def test_create_rule(client_with_api_key_write_permission: TestClient) -> None:
    response = client_with_api_key_write_permission.post(
        "/v0/rule-item",
        json={
            "$type": "AdditiveActivity",
            "start": ["2025"],
            "title": [{"value": "A new beginning", "language": "en"}],
        },
    )
    assert response.status_code == 200, response.text

    assert get_graph() == [
        {
            "fundingProgram": [],
            "start": ["2025"],
            "end": [],
            "theme": [],
            "label": "AdditiveActivity",
            "activityType": [],
        },
        {
            "start": "00000000000001",
            "end": "00000000000000",
            "label": "hadPrimarySource",
            "position": 0,
        },
        {
            "start": "AdditiveActivity",
            "end": "00000000000000",
            "label": "hadPrimarySource",
            "position": 0,
        },
        {
            "start": "00000000000001",
            "end": "00000000000000",
            "label": "stableTargetId",
            "position": 0,
        },
        {
            "start": "AdditiveActivity",
            "end": "Text",
            "label": "title",
            "position": 0,
        },
        {
            "start": "AdditiveActivity",
            "end": "bFQoRhcVH5DHUq",
            "label": "stableTargetId",
            "position": 0,
        },
        {
            "identifier": "00000000000000",
            "label": "MergedPrimarySource",
        },
        {
            "identifier": "00000000000001",
            "identifierInPrimarySource": "mex",
            "label": "ExtractedPrimarySource",
        },
        {
            "identifier": "bFQoRhcVH5DHUq",
            "label": "MergedActivity",
        },
        {
            "language": "en",
            "label": "Text",
            "value": "A new beginning",
        },
    ]


@pytest.mark.integration
def test_update_rule_missing_merged(
    client_with_api_key_write_permission: TestClient,
) -> None:
    response = client_with_api_key_write_permission.put(
        "/v0/rule-item/abc000abc000abc",
        json={
            "$type": "PreventiveActivity",
            "abstract": ["abc123abc123abc"],
        },
    )
    assert response.status_code == 412, response.text


@pytest.mark.usefixtures("load_dummy_data")
@pytest.mark.integration
def test_update_rule(client_with_api_key_write_permission: TestClient) -> None:
    response = client_with_api_key_write_permission.put(
        "/v0/rule-item/bFQoRhcVH5DHUB",
        json={
            "$type": "PreventiveActivity",
            "abstract": ["bFQoRhcVH5DHUr"],
        },
    )
    assert response.status_code == 200, response.text

    assert get_graph() == [
        {
            "identifier": "bFQoRhcVH5DHUA",
            "identifierInPrimarySource": "a-1",
            "fundingProgram": [],
            "start": [],
            "end": [],
            "theme": ["https://mex.rki.de/item/theme-3"],
            "label": "ExtractedActivity",
            "activityType": [],
        },
        {
            "identifier": "bFQoRhcVH5DHUw",
            "identifierInPrimarySource": "cp-2",
            "label": "ExtractedContactPoint",
            "email": ["help@contact-point.two"],
        },
        {
            "identifier": "bFQoRhcVH5DHUu",
            "identifierInPrimarySource": "cp-1",
            "label": "ExtractedContactPoint",
            "email": ["info@contact-point.one"],
        },
        {
            "identifier": "bFQoRhcVH5DHUy",
            "identifierInPrimarySource": "ou-1",
            "label": "ExtractedOrganizationalUnit",
            "email": [],
        },
        {
            "start": "00000000000001",
            "end": "00000000000000",
            "label": "hadPrimarySource",
            "position": 0,
        },
        {
            "start": "PreventiveActivity",
            "end": "00000000000000",
            "label": "hadPrimarySource",
            "position": 0,
        },
        {
            "start": "bFQoRhcVH5DHUq",
            "end": "00000000000000",
            "label": "hadPrimarySource",
            "position": 0,
        },
        {
            "start": "bFQoRhcVH5DHUs",
            "end": "00000000000000",
            "label": "hadPrimarySource",
            "position": 0,
        },
        {
            "start": "00000000000001",
            "end": "00000000000000",
            "label": "stableTargetId",
            "position": 0,
        },
        {"start": "bFQoRhcVH5DHUA", "end": "Link", "label": "website", "position": 0},
        {"start": "bFQoRhcVH5DHUA", "end": "Text", "label": "abstract", "position": 0},
        {"start": "bFQoRhcVH5DHUA", "end": "Text", "label": "abstract", "position": 1},
        {"start": "bFQoRhcVH5DHUy", "end": "Text", "label": "name", "position": 0},
        {"start": "bFQoRhcVH5DHUA", "end": "Text", "label": "title", "position": 0},
        {
            "start": "PreventiveActivity",
            "end": "bFQoRhcVH5DHUB",
            "label": "stableTargetId",
            "position": 0,
        },
        {
            "start": "bFQoRhcVH5DHUA",
            "end": "bFQoRhcVH5DHUB",
            "label": "stableTargetId",
            "position": 0,
        },
        {
            "start": "PreventiveActivity",
            "end": "bFQoRhcVH5DHUr",
            "label": "abstract",
            "position": 0,
        },
        {
            "start": "bFQoRhcVH5DHUA",
            "end": "bFQoRhcVH5DHUr",
            "label": "hadPrimarySource",
            "position": 0,
        },
        {
            "start": "bFQoRhcVH5DHUu",
            "end": "bFQoRhcVH5DHUr",
            "label": "hadPrimarySource",
            "position": 0,
        },
        {
            "start": "bFQoRhcVH5DHUw",
            "end": "bFQoRhcVH5DHUr",
            "label": "hadPrimarySource",
            "position": 0,
        },
        {
            "start": "bFQoRhcVH5DHUq",
            "end": "bFQoRhcVH5DHUr",
            "label": "stableTargetId",
            "position": 0,
        },
        {
            "start": "bFQoRhcVH5DHUy",
            "end": "bFQoRhcVH5DHUt",
            "label": "hadPrimarySource",
            "position": 0,
        },
        {
            "start": "bFQoRhcVH5DHUs",
            "end": "bFQoRhcVH5DHUt",
            "label": "stableTargetId",
            "position": 0,
        },
        {
            "start": "bFQoRhcVH5DHUA",
            "end": "bFQoRhcVH5DHUv",
            "label": "contact",
            "position": 0,
        },
        {
            "start": "bFQoRhcVH5DHUu",
            "end": "bFQoRhcVH5DHUv",
            "label": "stableTargetId",
            "position": 0,
        },
        {
            "start": "bFQoRhcVH5DHUA",
            "end": "bFQoRhcVH5DHUx",
            "label": "contact",
            "position": 1,
        },
        {
            "start": "bFQoRhcVH5DHUw",
            "end": "bFQoRhcVH5DHUx",
            "label": "stableTargetId",
            "position": 0,
        },
        {
            "start": "bFQoRhcVH5DHUA",
            "end": "bFQoRhcVH5DHUz",
            "label": "contact",
            "position": 2,
        },
        {
            "start": "bFQoRhcVH5DHUA",
            "end": "bFQoRhcVH5DHUz",
            "label": "responsibleUnit",
            "position": 0,
        },
        {
            "start": "bFQoRhcVH5DHUy",
            "end": "bFQoRhcVH5DHUz",
            "label": "stableTargetId",
            "position": 0,
        },
        {"identifier": "00000000000000", "label": "MergedPrimarySource"},
        {
            "identifier": "00000000000001",
            "identifierInPrimarySource": "mex",
            "label": "ExtractedPrimarySource",
        },
        {"identifier": "bFQoRhcVH5DHUB", "label": "MergedActivity"},
        {
            "identifier": "bFQoRhcVH5DHUq",
            "identifierInPrimarySource": "ps-1",
            "label": "ExtractedPrimarySource",
        },
        {"identifier": "bFQoRhcVH5DHUr", "label": "MergedPrimarySource"},
        {
            "identifier": "bFQoRhcVH5DHUs",
            "identifierInPrimarySource": "ps-2",
            "label": "ExtractedPrimarySource",
            "version": "Cool Version v2.13",
        },
        {"identifier": "bFQoRhcVH5DHUt", "label": "MergedPrimarySource"},
        {"identifier": "bFQoRhcVH5DHUv", "label": "MergedContactPoint"},
        {"identifier": "bFQoRhcVH5DHUx", "label": "MergedContactPoint"},
        {"identifier": "bFQoRhcVH5DHUz", "label": "MergedOrganizationalUnit"},
        {"label": "Link", "title": "Activity Homepage", "url": "https://activity-1"},
        {"label": "PreventiveActivity"},
        {"language": "de", "label": "Text", "value": "Aktivität 1"},
        {"language": "en", "label": "Text", "value": "An active activity."},
        {"language": "en", "label": "Text", "value": "Unit 1"},
        {"label": "Text", "value": "Une activité active."},
    ]
