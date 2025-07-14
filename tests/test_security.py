import pytest
from fastapi import HTTPException
from fastapi.security import HTTPBasicCredentials

from mex.backend.security import has_read_access, has_write_access

read_credentials = HTTPBasicCredentials(
    username="Reader",
    password="read_password",  # noqa: S106
)
write_credentials = HTTPBasicCredentials(
    username="Writer",
    password="write_password",  # noqa: S106
)
missing_user = HTTPBasicCredentials(
    username="Missing",
    password="no_password",  # noqa: S106
)
user_wrong_pw = HTTPBasicCredentials(
    username="Writer",
    password="wrong_password",  # noqa: S106
)


def test_has_write_access_with_api_key() -> None:
    has_write_access("write_key")
    with pytest.raises(HTTPException) as error:
        has_write_access(None)
    assert "Missing authentication" in error.value.detail

    with pytest.raises(HTTPException) as error:
        has_write_access("read_key")
    assert "Unauthorized API Key" in error.value.detail

    with pytest.raises(HTTPException) as error:
        has_write_access("moop")
    assert "Unauthorized API Key" in error.value.detail


def test_has_write_access_fails_if_key_and_credentials() -> None:
    with pytest.raises(HTTPException) as error:
        has_write_access("write_key", write_credentials)
    assert "Authenticate with X-API-Key or credentials" in error.value.detail


def test_has_write_access_with_basic_auth() -> None:
    has_write_access(credentials=write_credentials)
    with pytest.raises(HTTPException) as error:
        has_write_access(credentials=None)
    assert "Missing authentication" in error.value.detail

    with pytest.raises(HTTPException) as error:
        has_write_access(credentials=read_credentials)
    assert "Unauthorized credentials" in error.value.detail

    with pytest.raises(HTTPException) as error:
        has_write_access(credentials=user_wrong_pw)
    assert "Unauthorized credentials" in error.value.detail

    with pytest.raises(HTTPException) as error:
        has_write_access(credentials=missing_user)
    assert "Unauthorized credentials" in error.value.detail


def test_has_read_access_with_api_key() -> None:
    has_read_access("write_key")
    has_read_access("read_key")

    with pytest.raises(HTTPException) as error:
        has_read_access("moop")
    assert "Unauthorized API Key" in error.value.detail

    with pytest.raises(HTTPException) as error:
        has_read_access(None)
    assert "Missing authentication" in error.value.detail


def test_has_read_access_fails_if_key_and_credentials() -> None:
    with pytest.raises(HTTPException) as error:
        has_write_access("read_key", read_credentials)
    assert "Authenticate with X-API-Key or credentials" in error.value.detail


def test_has_read_access_with_basic_auth() -> None:
    has_read_access(credentials=write_credentials)
    has_read_access(credentials=read_credentials)

    with pytest.raises(HTTPException) as error:
        has_read_access(credentials=user_wrong_pw)
    assert "Unauthorized credentials" in error.value.detail

    with pytest.raises(HTTPException) as error:
        has_read_access(credentials=missing_user)
    assert "Unauthorized credentials" in error.value.detail

    with pytest.raises(HTTPException) as error:
        has_read_access(credentials=None)
    assert "Missing authentication" in error.value.detail
