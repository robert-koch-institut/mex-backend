from base64 import b64encode

import pytest
from fastapi.testclient import TestClient

from mex.backend.testing.main import app


@pytest.fixture
def client() -> TestClient:
    """Return a fastAPI test client initialized with our app."""
    with TestClient(app, raise_server_exceptions=False) as test_client:
        return test_client


@pytest.fixture
def client_with_basic_auth_write_permission(client: TestClient) -> TestClient:
    """Return a fastAPI test client with write permission granted by basic auth."""
    client.headers.update(
        {"Authorization": f"Basic {b64encode(b'Writer:write_password').decode()}"}
    )
    return client
