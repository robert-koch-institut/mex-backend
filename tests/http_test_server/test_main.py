import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

import pytest
from fastapi.testclient import TestClient

from mex.backend.http_test_server.main import app
from mex.backend.http_test_server.settings import HttpTestServerSettings
from mex.common.logging import logger

TEST_DATA_PATH = Path(__file__).parent / "test_data"


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
        str(TEST_DATA_PATH),
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


@dataclass
class SuccessResponseExpectation:
    mimetype: str = ""
    file_content: bytes = b""


@dataclass
class ErrorResoponseExpectation:
    detail_message: str = ""


@dataclass
class HttpTestServerExpectedResponse:
    status_code: int
    expecation: SuccessResponseExpectation | ErrorResoponseExpectation


def _read_file_content(file_path: str) -> bytes:
    with (TEST_DATA_PATH / file_path).open("rb") as f:
        return f.read()


successfull_json = HttpTestServerExpectedResponse(
    status_code=200,
    expecation=SuccessResponseExpectation(
        "application/json", file_content=_read_file_content("extractor/test_data.json")
    ),
)
successfull_csv = HttpTestServerExpectedResponse(
    status_code=200,
    expecation=SuccessResponseExpectation(
        "text/csv; charset=utf-8",
        file_content=_read_file_content("extractor/test_table.csv"),
    ),
)
not_existing_file = HttpTestServerExpectedResponse(
    status_code=404, expecation=ErrorResoponseExpectation("No files found")
)
too_many_file = HttpTestServerExpectedResponse(
    status_code=404, expecation=ErrorResoponseExpectation("Too many files found")
)


@pytest.mark.parametrize(
    ("method", "path", "expected_response"),
    [
        pytest.param(
            "GET", "extractor/test_data", successfull_json, id="successful_get_json"
        ),
        pytest.param(
            "POST", "extractor/test_data", successfull_json, id="successful_post_json"
        ),
        pytest.param(
            "GET", "extractor/test_table", successfull_csv, id="successful_get_csv"
        ),
        pytest.param(
            "POST", "extractor/test_table", successfull_csv, id="successful_post_csv"
        ),
        pytest.param(
            "GET",
            "extractor/not_existing",
            not_existing_file,
            id="error_get_file_not_found",
        ),
        pytest.param(
            "POST",
            "extractor/not_existing",
            not_existing_file,
            id="error_post_file_not_found",
        ),
        pytest.param(
            "GET",
            "extractor/too_many_files",
            too_many_file,
            id="error_get_too_many_files",
        ),
        pytest.param(
            "POST",
            "extractor/too_many_files",
            too_many_file,
            id="error_post_too_many_files",
        ),
    ],
)
def test_http_test_server(
    method: Literal["GET", "POST"],
    path: str,
    expected_response: HttpTestServerExpectedResponse,
    client: TestClient,
) -> None:
    url = f"/v0/http-test-server/{path}"
    response = client.request(url=url, method=method)
    assert response.status_code == expected_response.status_code
    if isinstance(expected_response.expecation, SuccessResponseExpectation):
        assert response.headers["content-type"] == expected_response.expecation.mimetype
        assert response.content == expected_response.expecation.file_content
    elif isinstance(expected_response.expecation, ErrorResoponseExpectation):
        assert response.json() == {
            "detail": expected_response.expecation.detail_message
        }
