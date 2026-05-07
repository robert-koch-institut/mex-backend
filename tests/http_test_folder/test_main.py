
import logging
from pathlib import Path

from fastapi.testclient import TestClient
import pytest
from mex.backend.http_test_server.main import app
from mex.backend.http_test_server.settings import HttpTestServerSettings

from mex.common.logging import logger


@pytest.fixture
def log_level(request: pytest.FixtureRequest) -> int:
    """Returns a sensible log-level for the current pytest verbosity.

    This can be controlled by adding more "v"s to `pytest -v`.
    """
    levels_by_verbosity = {
        0: logging.ERROR,  # always shown
        1: logging.WARNING,  # -v
        2: logging.INFO,  # -vv
    }
    return levels_by_verbosity.get(
        request.config.option.verbose,
        logging.DEBUG,  # `-vvv` and above
    )


@pytest.fixture(autouse=True)
def settings(
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
    log_level: int,
) -> HttpTestServerSettings:
    """Load the settings for this pytest session."""
    
    monkeypatch.setenv(
        "MEX_HTTP_TEST_SERVER_TEST_DATA_DIRECTORY",
        Path(__file__).parent / 'test_data'
    )
    # temporarily reduce log-level because the settings emit their configuration
    # on every instantiation or value-change. this would flood the test logs with noise,
    # especially because this fixture is used by *every* test.
    with caplog.at_level(log_level, logger=logger.name):
        return HttpTestServerSettings.get()


@pytest.fixture
def client() -> TestClient:
    """Return a fastAPI test client initialized with our app."""
    with TestClient(app, raise_server_exceptions=False) as test_client:
        return test_client


# TODO(MX-2250): finish tests
# @pytest.mark.parametrize(
#     (   id:"successful_get",
#         id:"successful_post",
#         id:"error_get_file_not_found",
#         id:"error_post_file_not_found",
#         id:"error_get_too_many_files",
#         id:"error_post_too_many_files"
#         )
# )
def test_http_test_server(
    client: TestClient,
    method: "GET"|"POST", 
    path: str, 
    expected_status_code: int, 
    expected_mimetype: str, 
    expected_file_content: bytes,
) -> None:
    url = f"/v0/http-test-server/{path}"
    response = client.request(url=url, method=method)
    assert response.status_code == expected_status_code
    assert response
    assert response.headers["content-type"] == expected_mimetype
    assert response.content == expected_file_content