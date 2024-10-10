import json
from typing import Any
from unittest.mock import Mock

import pydantic_core
import pytest
from pydantic import ValidationError

from mex.backend.exceptions import handle_uncaught_exception, handle_validation_error
from mex.common.exceptions import MExError

MOCK_REQUEST_SCOPE = {
    "http_version": "1.1",
    "method": "GET",
    "path": "/test",
    "path_params": {},
    "query_string": "",
    "scheme": "HTTPS",
}


@pytest.mark.parametrize(
    ("exception", "expected", "status_code"),
    [
        (
            TypeError("foo"),
            {
                "debug": {
                    "errors": [{"type": "TypeError"}],
                    "scope": MOCK_REQUEST_SCOPE,
                },
                "message": "foo",
            },
            500,
        ),
        (
            MExError("bar"),
            {
                "message": "MExError: bar ",
                "debug": {
                    "errors": [{"type": "MExError"}],
                    "scope": MOCK_REQUEST_SCOPE,
                },
            },
            500,
        ),
    ],
    ids=["TypeError", "MExError"],
)
def test_handle_uncaught_exception(
    exception: Exception, expected: dict[str, Any], status_code: int
) -> None:
    request = Mock(scope=MOCK_REQUEST_SCOPE)
    response = handle_uncaught_exception(request, exception)
    assert response.status_code == status_code, response.body
    assert json.loads(response.body) == expected


def test_handle_validation_error() -> None:
    request = Mock(scope=MOCK_REQUEST_SCOPE)
    exception = ValidationError.from_exception_data(
        "foo",
        [
            {
                "type": pydantic_core.PydanticCustomError(
                    "TestError", "You messed up!"
                ),
                "loc": ("integerAttribute",),
                "input": "mumbo-jumbo",
            }
        ],
    )
    response = handle_validation_error(request, exception)
    assert response.status_code == 400, response.body
    assert json.loads(response.body) == {
        "debug": {
            "errors": [
                {
                    "input": "mumbo-jumbo",
                    "loc": ["integerAttribute"],
                    "msg": "You messed up!",
                    "type": "TestError",
                }
            ],
            "scope": MOCK_REQUEST_SCOPE,
        },
        "message": "1 validation error for foo\n"
        "integerAttribute\n"
        "  You messed up! [type=TestError, input_value='mumbo-jumbo', "
        "input_type=str]",
    }
