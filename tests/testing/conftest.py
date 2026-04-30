import pytest
from fastapi.testclient import TestClient

from mex.backend.testing.main import app


@pytest.fixture
def client() -> TestClient:
    """Return a fastAPI test client initialized with our app."""
    with TestClient(app, raise_server_exceptions=False) as test_client:
        return test_client


@pytest.fixture
def client_with_bearer_write_permission(client: TestClient) -> TestClient:
    """Return a fastAPI test client with write permission granted by Bearer token."""
    client.headers.update({"Authorization": "Bearer Writer"})
    return client
