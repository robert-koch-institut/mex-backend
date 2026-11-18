import pytest
from fastapi import HTTPException

from mex.backend.testing.security import has_oidc_access_mocked


def test_has_oidc_access_mocked_success() -> None:
    result = has_oidc_access_mocked(token="Writer")  # noqa: S106
    assert result == "Writer"


def test_has_oidc_access_mocked_missing_credentials() -> None:
    with pytest.raises(HTTPException) as error:
        has_oidc_access_mocked(token=None)
    assert error.value.status_code == 401
    assert "Missing Bearer token" in error.value.detail
