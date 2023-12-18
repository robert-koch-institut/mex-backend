import pytest
from fastapi import HTTPException

from mex.backend.security import has_read_access, has_write_access


def test_has_write_access() -> None:
    assert has_write_access("write_key") is None
    with pytest.raises(HTTPException) as error:
        has_write_access(None)
    assert "Missing authentication" in error.value.detail

    with pytest.raises(HTTPException) as error:
        has_write_access("read_key")
    assert "Unauthorized API Key" in error.value.detail

    with pytest.raises(HTTPException) as error:
        has_write_access("moop")
    assert "Unauthorized API Key" in error.value.detail


def test_has_read_access() -> None:
    assert has_read_access("write_key") is None
    assert has_read_access("read_key") is None

    with pytest.raises(HTTPException) as error:
        has_read_access("moop")
    assert "Unauthorized API Key" in error.value.detail

    with pytest.raises(HTTPException) as error:
        has_read_access(None)
    assert "Missing authentication" in error.value.detail
