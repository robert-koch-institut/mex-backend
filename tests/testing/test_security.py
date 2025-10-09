import pytest
from fastapi import HTTPException
from fastapi.security import HTTPBasicCredentials

from mex.backend.testing.security import (
    _check_header_for_authorization_method,
    has_write_access_ldap,
)

write_credentials = HTTPBasicCredentials(
    username="Writer@rki.de",
    password="write_password",  # noqa: S106
)


def test_has_write_access_ldap_success() -> None:
    # Should return the escaped username part
    result = has_write_access_ldap(credentials=write_credentials)
    assert result == "Writer"


def test_has_write_access_ldap_missing_credentials() -> None:
    with pytest.raises(HTTPException) as error:
        has_write_access_ldap(credentials=None)  # type: ignore [arg-type]
    assert error.value.status_code == 401
    assert "Missing authentication header" in error.value.detail


def test_check_header_for_authorization_method_missing_both() -> None:
    with pytest.raises(HTTPException) as exc:
        _check_header_for_authorization_method(api_key=None, credentials=None)
    assert exc.value.status_code == 401
    assert "Missing authentication header" in exc.value.detail


def test_check_header_for_authorization_method_both_present() -> None:
    credentials = HTTPBasicCredentials(username="Writer", password="write_password")  # noqa: S106
    with pytest.raises(HTTPException) as exc:
        _check_header_for_authorization_method(
            api_key="some-key", credentials=credentials
        )
    assert exc.value.status_code == 401
    assert "Authenticate with X-API-Key or credentials, not both." in exc.value.detail
