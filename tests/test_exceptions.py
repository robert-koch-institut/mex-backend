import json
from typing import Any
from unittest.mock import Mock

import pytest
from pydantic import BaseModel, ValidationError
from starlette import status

from mex.backend.exceptions import handle_detailed_error, handle_uncaught_exception
from mex.backend.graph.exceptions import InconsistentGraphError
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
            status.HTTP_500_INTERNAL_SERVER_ERROR,
        ),
        (
            MExError("bar"),
            {
                "message": "MExError: bar",
                "debug": {
                    "errors": [{"type": "MExError"}],
                    "scope": MOCK_REQUEST_SCOPE,
                },
            },
            status.HTTP_500_INTERNAL_SERVER_ERROR,
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
    assert json.loads(bytes(response.body)) == expected


class DummyModel(BaseModel):
    numbers: list[int]


try:
    DummyModel.model_validate({"numbers": "foo"})
except ValidationError as error:
    validation_error = error


try:
    try:
        DummyModel.model_validate({"numbers": "foo"})
    except ValidationError as error:
        msg = "this has to validate"
        raise InconsistentGraphError(msg) from error
except InconsistentGraphError as error:
    inconsistent_graph_error = error


@pytest.mark.parametrize(
    ("exception", "expected"),
    [
        (
            validation_error,
            {
                "message": """1 validation error for DummyModel
numbers
  Input should be a valid list [type=list_type, input_value='foo', input_type=str]
    For further information visit https://errors.pydantic.dev/2.9/v/list_type""",
                "debug": {
                    "errors": [
                        {
                            "type": "list_type",
                            "loc": ["numbers"],
                            "msg": "Input should be a valid list",
                            "input": "foo",
                            "url": "https://errors.pydantic.dev/2.9/v/list_type",
                        }
                    ],
                    "scope": MOCK_REQUEST_SCOPE,
                },
            },
        ),
        (
            inconsistent_graph_error,
            {
                "message": "InconsistentGraphError: this has to validate",
                "debug": {
                    "errors": [
                        {
                            "type": "list_type",
                            "loc": ["numbers"],
                            "msg": "Input should be a valid list",
                            "input": "foo",
                            "url": "https://errors.pydantic.dev/2.9/v/list_type",
                        }
                    ],
                    "scope": MOCK_REQUEST_SCOPE,
                },
            },
        ),
    ],
    ids=["ValidationError", "InconsistentGraphError"],
)
def test_handle_detailed_error(exception: Exception, expected: dict[str, Any]) -> None:
    request = Mock(scope=MOCK_REQUEST_SCOPE)
    response = handle_detailed_error(request, exception)
    assert response.status_code == status.HTTP_400_BAD_REQUEST, response.body
    assert json.loads(bytes(response.body)) == expected
