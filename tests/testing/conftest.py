import pytest
from fastapi.testclient import TestClient

from mex.backend.testing.main import app

pytest_plugins = ("mex.common.testing.plugin",)


@pytest.fixture
def client() -> TestClient:
    """Return a fastAPI test client initialized with our app."""
    with TestClient(app, raise_server_exceptions=False) as test_client:
        return test_client
