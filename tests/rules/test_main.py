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
