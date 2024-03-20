from fastapi.testclient import TestClient


def test_create_rule_set(client_with_api_key_write_permission: TestClient) -> None:
    response = client_with_api_key_write_permission.post(
        "/v0/rule-set",
        json={"title": {"addValues": [{"value": "A new beginning", "language": "en"}]}},
    )
    assert response.status_code == 201, response.text
    response_json = response.json()
    assert set(response_json) == {
        "addValues",
        "blockValues",
        "blockPrimarySources",
    }
    assert response_json["addValues"]["title"] == [
        {"value": "A new beginning", "language": "en"}
    ]
