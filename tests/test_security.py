from unittest.mock import patch

import pytest
from fastapi import HTTPException
from fastapi.security import HTTPBasicCredentials
from ldap3.core.exceptions import LDAPBindError

from mex.backend.security import (
    has_read_access,
    has_write_access,
    has_write_access_ldap,
)

write_credentials = HTTPBasicCredentials(
    username="Writer",
    password="write_password",  # noqa: S106
)
user_wrong_pw = HTTPBasicCredentials(
    username="Writer",
    password="wrong_password",  # noqa: S106
)


def test_has_write_access_with_api_key() -> None:
    has_write_access("write_key")
    with pytest.raises(HTTPException) as error:
        has_write_access(None)
    assert "Missing X-API-Key" in error.value.detail

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

    with pytest.raises(HTTPException) as error:
        has_read_access(None)
    assert "Missing X-API-Key" in error.value.detail


def test_has_write_access_ldap_success() -> None:
    with patch("mex.backend.security.BackendSettings.get") as mock_settings:
        mock_settings.return_value.ldap_url.get_secret_value.return_value = (
            "ldaps://ldap.example.com:636"
        )
        with patch("mex.backend.security.Connection") as mock_connection:
            mocked_connection = mock_connection.return_value.__enter__.return_value
            mocked_connection.server.check_availability.return_value = True
            assert write_credentials.username == has_write_access_ldap(
                credentials=write_credentials
            )


def test_has_write_access_ldap_bind_error() -> None:
    with patch("mex.backend.security.BackendSettings.get") as mock_settings:
        mock_settings.return_value.ldap_url.get_secret_value.return_value = (
            "ldaps://ldap.example.com:636"
        )
        with patch(
            "mex.backend.security.Connection", side_effect=LDAPBindError("fail")
        ):
            with pytest.raises(HTTPException) as error:
                has_write_access_ldap(credentials=user_wrong_pw)
            assert error.value.status_code == 401
            assert "LDAP bind failed." in error.value.detail


def test_has_write_access_ldap_server_not_available() -> None:
    with patch("mex.backend.security.BackendSettings.get") as mock_settings:
        mock_settings.return_value.ldap_url.get_secret_value.return_value = (
            "ldaps://ldap.example.com:636"
        )
        with patch("mex.backend.security.Connection") as mock_connection:
            mocked_connection = mock_connection.return_value.__enter__.return_value
            mocked_connection.server.check_availability.return_value = False
            with pytest.raises(HTTPException) as error:
                has_write_access_ldap(credentials=write_credentials)
            assert error.value.status_code == 503
            assert "LDAP server not available." in error.value.detail
