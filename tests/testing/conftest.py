import pytest
from fastapi.testclient import TestClient

from mex.backend.testing.main import app as testing_app


@pytest.fixture
def testing_app_client() -> TestClient:
    """Return a fastAPI test client initialized with our app."""
    with TestClient(testing_app, raise_server_exceptions=False) as test_client:
        return test_client


@pytest.fixture
def testing_app_client_with_api_key_write_permission(
    testing_app_client: TestClient,
) -> TestClient:
    """Return a fastAPI test client with write permission initialized with our app."""
    testing_app_client.headers.update({"X-API-Key": "write_key"})
    return testing_app_client


@pytest.fixture
def testing_app_client_with_api_key_read_permission(
    testing_app_client: TestClient,
) -> TestClient:
    """Return a fastAPI test client with read permission granted by API key."""
    testing_app_client.headers.update({"X-API-Key": "read_key"})
    return testing_app_client


@pytest.fixture
def testing_app_client_that_is_ldap_authenticated(
    testing_app_client: TestClient,
) -> TestClient:
    """Return a fastAPI test client with write permission granted by basic auth."""
    testing_app_client.auth = ("Writer", "write_password")
    return testing_app_client
