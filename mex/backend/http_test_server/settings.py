from pydantic import Field

from mex.common.settings import BaseSettings
from mex.common.types import AssetsPath


class HttpTestServerSettings(BaseSettings):
    """Settings definition for the http_test_server."""

    http_test_server_host: str = Field(
        "localhost",
        min_length=1,
        max_length=250,
        description="Host that the http_test_server will run on.",
        validation_alias="MEX_HTTP_TEST_SERVER_HOST",
    )
    http_test_server_port: int = Field(
        8088,
        gt=0,
        lt=65536,
        description="Port that the http_test_server should listen on.",
        validation_alias="MEX_HTTP_TEST_SERVER_PORT",
    )
    http_test_server_root_path: str = Field(
        "",
        description="Root path that the http_test_server should run under.",
        validation_alias="MEX_HTTP_TEST_SERVER_ROOT_PATH",
    )
    http_test_server_test_data_directory: AssetsPath = Field(
        AssetsPath("http-test-server-data"),
        description="Directory that the http_test_server should return test data from.",
        validation_alias="MEX_HTTP_TEST_SERVER_TEST_DATA_DIRECTORY",
    )
