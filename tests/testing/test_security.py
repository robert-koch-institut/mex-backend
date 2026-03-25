import pytest
from fastapi import HTTPException
from fastapi.security import HTTPAuthorizationCredentials

from mex.backend.testing.security import has_oidc_access_mocked

write_bearer = HTTPAuthorizationCredentials(
    scheme="Bearer",
    credentials="Writer",
)


def test_has_oidc_access_mocked_success() -> None:
    result = has_oidc_access_mocked(credentials=write_bearer)
    assert result == "Writer"


def test_has_oidc_access_mocked_missing_credentials() -> None:
    with pytest.raises(HTTPException) as error:
        has_oidc_access_mocked(credentials=None)
    assert error.value.status_code == 401
    assert "Missing Bearer token" in error.value.detail
