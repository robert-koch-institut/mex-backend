from unittest.mock import MagicMock, patch

import jwt as jwt_lib
import pytest
from fastapi import HTTPException

from mex.backend.security import (
    _get_jwks_client,
    has_oidc_access,
    has_read_access,
    has_write_access,
)

FAKE_TOKEN = "fake.jwt.token"  # noqa: S105


@pytest.fixture(autouse=True)
def reset_jwks_client() -> None:
    """Reset the cached JWKS client between tests."""
    _get_jwks_client.cache_clear()


def _mock_jwks_and_decode(claims: dict) -> tuple:  # type: ignore[type-arg]
    """Return context managers that mock JWKS client and jwt.decode with given claims."""
    mock_client = MagicMock()
    mock_client.get_signing_key_from_jwt.return_value = MagicMock(key="fake_key")
    return (
        patch("mex.backend.security._get_jwks_client", return_value=mock_client),
        patch("mex.backend.security.jwt.decode", return_value=claims),
    )


def test_has_oidc_access_no_bearer() -> None:
    with pytest.raises(HTTPException) as exc:
        has_oidc_access(token=None)
    assert exc.value.status_code == 401
    assert "Missing Bearer token" in exc.value.detail


def test_has_oidc_access_api_key_rejected() -> None:
    with pytest.raises(HTTPException) as exc:
        has_oidc_access(api_key="write_key")
    assert exc.value.status_code == 400
    assert "requires a Bearer token" in exc.value.detail


def test_has_oidc_access_both_credentials_rejected() -> None:
    with pytest.raises(HTTPException) as exc:
        has_oidc_access(api_key="write_key", token=FAKE_TOKEN)
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
        has_oidc_access(token=FAKE_TOKEN)
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
        has_oidc_access(token=FAKE_TOKEN)
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
        has_oidc_access(token=FAKE_TOKEN)
    assert exc.value.status_code == 401


def test_has_oidc_access_no_group() -> None:
    mock_jwks, mock_decode = _mock_jwks_and_decode(
        {"preferred_username": "nobody", "groups": []}
    )
    with mock_jwks, mock_decode, pytest.raises(HTTPException) as exc:
        has_oidc_access(token=FAKE_TOKEN)
    assert exc.value.status_code == 403
    assert "read-level" in exc.value.detail


def test_has_oidc_access_missing_preferred_username() -> None:
    mock_jwks, mock_decode = _mock_jwks_and_decode({"groups": ["Abteilung_21"]})
    with mock_jwks, mock_decode, pytest.raises(HTTPException) as exc:
        has_oidc_access(token=FAKE_TOKEN)
    assert exc.value.status_code == 401
    assert "preferred_username" in exc.value.detail


def test_has_oidc_access_read_group_success() -> None:
    mock_jwks, mock_decode = _mock_jwks_and_decode(
        {"preferred_username": "MoritzE", "groups": ["Fachgebiet_99"]}
    )
    with mock_jwks, mock_decode:
        result = has_oidc_access(token=FAKE_TOKEN)
    assert result == "MoritzE"


def test_has_oidc_access_write_group_success() -> None:
    mock_jwks, mock_decode = _mock_jwks_and_decode(
        {"preferred_username": "MaxM", "groups": ["Abteilung_21"]}
    )
    with mock_jwks, mock_decode:
        result = has_oidc_access(token=FAKE_TOKEN)
    assert result == "MaxM"


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
        has_write_access("write_key", FAKE_TOKEN)
    assert exc.value.status_code == 400


def test_has_write_access_bearer_write_group() -> None:
    mock_jwks, mock_decode = _mock_jwks_and_decode(
        {"preferred_username": "MaxM", "groups": ["Abteilung_21"]}
    )
    with mock_jwks, mock_decode:
        has_write_access(api_key=None, token=FAKE_TOKEN)


def test_has_write_access_bearer_no_write_group() -> None:
    mock_jwks, mock_decode = _mock_jwks_and_decode(
        {"preferred_username": "MoritzE", "groups": ["Fachgebiet_99"]}
    )
    with mock_jwks, mock_decode, pytest.raises(HTTPException) as exc:
        has_write_access(api_key=None, token=FAKE_TOKEN)
    assert exc.value.status_code == 403


def test_has_read_access_both_credentials_rejected() -> None:
    with pytest.raises(HTTPException) as exc:
        has_read_access("read_key", FAKE_TOKEN)
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
        has_read_access(api_key=None, token=FAKE_TOKEN)


def test_has_read_access_bearer_write_group_implies_read() -> None:
    mock_jwks, mock_decode = _mock_jwks_and_decode(
        {"preferred_username": "MaxM", "groups": ["Abteilung_21"]}
    )
    with mock_jwks, mock_decode:
        has_read_access(api_key=None, token=FAKE_TOKEN)


def test_has_read_access_bearer_no_group() -> None:
    mock_jwks, mock_decode = _mock_jwks_and_decode(
        {"preferred_username": "nobody", "groups": []}
    )
    with mock_jwks, mock_decode, pytest.raises(HTTPException) as exc:
        has_read_access(api_key=None, token=FAKE_TOKEN)
    assert exc.value.status_code == 403


def test_get_jwks_client_caches_per_uri() -> None:
    with patch("mex.backend.security.PyJWKClient") as mock_client_cls:
        mock_client_cls.side_effect = lambda uri, lifespan: MagicMock(
            uri=uri, lifespan=lifespan
        )

        first = _get_jwks_client("https://issuer.example/keys")
        second = _get_jwks_client("https://issuer.example/keys")
        third = _get_jwks_client("https://other.example/keys")

    assert first is second
    assert first is not third
    assert mock_client_cls.call_count == 2
    mock_client_cls.assert_any_call("https://issuer.example/keys", lifespan=3600)
    mock_client_cls.assert_any_call("https://other.example/keys", lifespan=3600)
