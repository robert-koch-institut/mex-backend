from typing import Annotated
from unittest.mock import patch

import pytest
from fastapi import Depends, FastAPI, HTTPException
from fastapi.security import HTTPBasicCredentials
from ldap3.core.exceptions import LDAPBindError
from starlette.testclient import TestClient

from mex.backend.security import (
    HTTP_BASIC_AUTH,
    X_API_KEY,
    has_read_access,
    has_write_access,
    is_ldap_authenticated,
)

write_credentials = HTTPBasicCredentials(
    username="Writer",
    password="write_password",  # noqa: S106
)
user_wrong_pw = HTTPBasicCredentials(
    username="Writer",
    password="wrong_password",  # noqa: S106
)


app = FastAPI()


@app.get("/test_x_api_key_dependency")
def route_for_testing_x_api_key_dependency(
    auth: Annotated[str, Depends(X_API_KEY)], expected_x_api_key: str
) -> dict[str, str]:
    assert auth == expected_x_api_key
    return {"status": "ok"}


@app.get("/test_http_basic_auth_dependency")
def route_for_testing_http_basic_auth_dependency(
    credentials: Annotated[HTTPBasicCredentials, Depends(HTTP_BASIC_AUTH)],
    expected_username: str,
    expected_password: str,
) -> dict[str, str]:
    assert credentials.username == expected_username
    assert credentials.password == expected_password
    return {"status": "ok"}


client = TestClient(
    app,
)


def test_missing_x_api_key_header_returns_401() -> None:
    response = client.get("/test_x_api_key_dependency")  # No X-API-Key header
    assert response.status_code == 401, response.text


def test_present_x_api_key_header_returns_200() -> None:
    key = "foo"
    response = client.get(
        "/test_x_api_key_dependency",
        params={"expected_x_api_key": key},
        headers={"X-API-Key": key},
    )
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_missing_http_basic_auth_returns_401() -> None:
    response = client.get("/test_http_basic_auth_dependency")  # No X-API-Key header
    assert response.status_code == 401, response.text


def test_present_http_basic_auth_returns_200() -> None:
    username = "foo"
    password = "bar"  # noqa: S105
    response = client.get(
        "/test_http_basic_auth_dependency",
        params={"expected_username": username, "expected_password": password},
        auth=(username, password),
    )
    assert response.status_code == 200, response.text
    assert response.json() == {"status": "ok"}


def test_has_write_access_with_api_key() -> None:
    has_write_access("write_key")

    with pytest.raises(HTTPException) as error:
        has_write_access("read_key")
    assert "Unauthorized API Key" in error.value.detail

    with pytest.raises(HTTPException) as error:
        has_write_access("moop")
    assert "Unauthorized API Key" in error.value.detail


def test_has_read_access_with_api_key() -> None:
    has_read_access("write_key")
    has_read_access("read_key")

    with pytest.raises(HTTPException) as error:
        has_read_access("moop")
    assert "Unauthorized API Key" in error.value.detail


def test_is_ldap_authenticated_success() -> None:
    with patch("mex.backend.security.BackendSettings.get") as mock_settings:
        mock_settings.return_value.ldap_url.get_secret_value.return_value = (
            "ldaps://ldap.example.com:636"
        )
        with patch("mex.backend.security.Connection") as mock_connection:
            mocked_connection = mock_connection.return_value.__enter__.return_value
            mocked_connection.server.check_availability.return_value = True
            assert write_credentials.username == is_ldap_authenticated(
                credentials=write_credentials
            )


def test_is_ldap_authenticated_bind_error() -> None:
    with patch("mex.backend.security.BackendSettings.get") as mock_settings:
        mock_settings.return_value.ldap_url.get_secret_value.return_value = (
            "ldaps://ldap.example.com:636"
        )
        with patch(
            "mex.backend.security.Connection", side_effect=LDAPBindError("fail")
        ):
            with pytest.raises(HTTPException) as error:
                is_ldap_authenticated(credentials=user_wrong_pw)
            assert error.value.status_code == 401
            assert "LDAP bind failed." in error.value.detail


def test_is_ldap_authenticated_server_not_available() -> None:
    with patch("mex.backend.security.BackendSettings.get") as mock_settings:
        mock_settings.return_value.ldap_url.get_secret_value.return_value = (
            "ldaps://ldap.example.com:636"
        )
        with patch("mex.backend.security.Connection") as mock_connection:
            mocked_connection = mock_connection.return_value.__enter__.return_value
            mocked_connection.server.check_availability.return_value = False
            with pytest.raises(HTTPException) as error:
                is_ldap_authenticated(credentials=write_credentials)
            assert error.value.status_code == 503
            assert "LDAP server not available." in error.value.detail
