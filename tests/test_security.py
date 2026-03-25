from unittest.mock import MagicMock, patch

import jwt as jwt_lib
import pytest
from fastapi import HTTPException
from fastapi.security import HTTPAuthorizationCredentials
from pytest import MonkeyPatch

import mex.backend.security as security_module
from mex.backend.security import (
    has_oidc_access,
    has_read_access,
    has_write_access,
)


@pytest.fixture(autouse=True)
def reset_jwks_client(monkeypatch: MonkeyPatch) -> None:
    """Reset the cached JWKS client between tests."""
    monkeypatch.setattr(security_module, "_jwks_client", None)


_write_bearer = HTTPAuthorizationCredentials(
    scheme="Bearer", credentials="fake.jwt.token"
)
_read_bearer = HTTPAuthorizationCredentials(
    scheme="Bearer", credentials="fake.jwt.token"
)


def _mock_jwks_and_decode(claims: dict) -> tuple:  # type: ignore[type-arg]
    """Return context managers that mock JWKS client and jwt.decode with given claims."""
    mock_client = MagicMock()
    mock_client.get_signing_key_from_jwt.return_value = MagicMock(key="fake_key")
    return (
        patch("mex.backend.security._get_jwks_client", return_value=mock_client),
        patch("mex.backend.security.jwt.decode", return_value=claims),
    )


# --- has_oidc_access ---


def test_has_oidc_access_no_bearer() -> None:
    with pytest.raises(HTTPException) as exc:
        has_oidc_access(credentials=None)
    assert exc.value.status_code == 401
    assert "Missing Bearer token" in exc.value.detail


def test_has_oidc_access_api_key_rejected() -> None:
    with pytest.raises(HTTPException) as exc:
        has_oidc_access(api_key="write_key")
    assert exc.value.status_code == 400
    assert "requires a Bearer token" in exc.value.detail


def test_has_oidc_access_both_credentials_rejected() -> None:
    with pytest.raises(HTTPException) as exc:
        has_oidc_access(api_key="write_key", credentials=_write_bearer)
    assert exc.value.status_code == 400


def test_has_oidc_access_jwks_unavailable() -> None:
    mock_client = MagicMock()
    mock_client.get_signing_key_from_jwt.side_effect = jwt_lib.PyJWKClientError(
        "connection refused"
    )
    with (
        patch("mex.backend.security._get_jwks_client", return_value=mock_client),
        pytest.raises(HTTPException) as exc,
    ):
        has_oidc_access(credentials=_write_bearer)
    assert exc.value.status_code == 503


def test_has_oidc_access_expired_token() -> None:
    mock_client = MagicMock()
    mock_client.get_signing_key_from_jwt.return_value = MagicMock(key="fake_key")
    with (
        patch("mex.backend.security._get_jwks_client", return_value=mock_client),
        patch(
            "mex.backend.security.jwt.decode",
            side_effect=jwt_lib.ExpiredSignatureError("expired"),
        ),
        pytest.raises(HTTPException) as exc,
    ):
        has_oidc_access(credentials=_write_bearer)
    assert exc.value.status_code == 401
    assert "expired" in exc.value.detail.lower()


def test_has_oidc_access_invalid_token() -> None:
    mock_client = MagicMock()
    mock_client.get_signing_key_from_jwt.return_value = MagicMock(key="fake_key")
    with (
        patch("mex.backend.security._get_jwks_client", return_value=mock_client),
        patch(
            "mex.backend.security.jwt.decode",
            side_effect=jwt_lib.InvalidTokenError("bad signature"),
        ),
        pytest.raises(HTTPException) as exc,
    ):
        has_oidc_access(credentials=_write_bearer)
    assert exc.value.status_code == 401


def test_has_oidc_access_no_group() -> None:
    mock_jwks, mock_decode = _mock_jwks_and_decode(
        {"preferred_username": "nobody", "groups": []}
    )
    with mock_jwks, mock_decode, pytest.raises(HTTPException) as exc:
        has_oidc_access(credentials=_write_bearer)
    assert exc.value.status_code == 403
    assert "read-level" in exc.value.detail


def test_has_oidc_access_missing_preferred_username() -> None:
    mock_jwks, mock_decode = _mock_jwks_and_decode({"groups": ["Abteilung_21"]})
    with mock_jwks, mock_decode, pytest.raises(HTTPException) as exc:
        has_oidc_access(credentials=_write_bearer)
    assert exc.value.status_code == 401
    assert "Invalid token claims" in exc.value.detail


def test_has_oidc_access_read_group_success() -> None:
    mock_jwks, mock_decode = _mock_jwks_and_decode(
        {"preferred_username": "MoritzE", "groups": ["Fachgebiet_99"]}
    )
    with mock_jwks, mock_decode:
        result = has_oidc_access(credentials=_read_bearer)
    assert result == "MoritzE"


def test_has_oidc_access_write_group_success() -> None:
    mock_jwks, mock_decode = _mock_jwks_and_decode(
        {"preferred_username": "MaxM", "groups": ["Abteilung_21"]}
    )
    with mock_jwks, mock_decode:
        result = has_oidc_access(credentials=_write_bearer)
    assert result == "MaxM"


# --- has_write_access (JWT path) ---


def test_has_write_access_with_api_key() -> None:
    has_write_access("write_key")
    with pytest.raises(HTTPException) as error:
        has_write_access(None, None)
    assert "Missing credentials" in error.value.detail

    with pytest.raises(HTTPException) as error:
        has_write_access("read_key")
    assert "Unauthorized API Key" in error.value.detail

    with pytest.raises(HTTPException) as error:
        has_write_access("moop")
    assert "Unauthorized API Key" in error.value.detail


def test_has_write_access_both_credentials_rejected() -> None:
    with pytest.raises(HTTPException) as exc:
        has_write_access("write_key", _write_bearer)
    assert exc.value.status_code == 400


def test_has_write_access_bearer_write_group() -> None:
    mock_jwks, mock_decode = _mock_jwks_and_decode(
        {"preferred_username": "MaxM", "groups": ["Abteilung_21"]}
    )
    with mock_jwks, mock_decode:
        has_write_access(api_key=None, credentials=_write_bearer)


def test_has_write_access_bearer_no_write_group() -> None:
    mock_jwks, mock_decode = _mock_jwks_and_decode(
        {"preferred_username": "MoritzE", "groups": ["Fachgebiet_99"]}
    )
    with mock_jwks, mock_decode, pytest.raises(HTTPException) as exc:
        has_write_access(api_key=None, credentials=_read_bearer)
    assert exc.value.status_code == 403


# --- has_read_access (JWT path) ---


def test_has_read_access_both_credentials_rejected() -> None:
    with pytest.raises(HTTPException) as exc:
        has_read_access("read_key", _read_bearer)
    assert exc.value.status_code == 400


def test_has_read_access_with_api_key() -> None:
    has_read_access("write_key")
    has_read_access("read_key")

    with pytest.raises(HTTPException) as error:
        has_read_access("moop")
    assert "Unauthorized API Key" in error.value.detail

    with pytest.raises(HTTPException) as error:
        has_read_access(None, None)
    assert "Missing credentials" in error.value.detail


def test_has_read_access_bearer_read_group() -> None:
    mock_jwks, mock_decode = _mock_jwks_and_decode(
        {"preferred_username": "MoritzE", "groups": ["Fachgebiet_99"]}
    )
    with mock_jwks, mock_decode:
        has_read_access(api_key=None, credentials=_read_bearer)


def test_has_read_access_bearer_write_group_implies_read() -> None:
    mock_jwks, mock_decode = _mock_jwks_and_decode(
        {"preferred_username": "MaxM", "groups": ["Abteilung_21"]}
    )
    with mock_jwks, mock_decode:
        has_read_access(api_key=None, credentials=_read_bearer)


def test_has_read_access_bearer_no_group() -> None:
    mock_jwks, mock_decode = _mock_jwks_and_decode(
        {"preferred_username": "nobody", "groups": []}
    )
    with mock_jwks, mock_decode, pytest.raises(HTTPException) as exc:
        has_read_access(api_key=None, credentials=_read_bearer)
    assert exc.value.status_code == 403
