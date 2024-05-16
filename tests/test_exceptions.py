import json
from typing import Any
from unittest.mock import Mock

import pydantic_core
import pytest
from pydantic import ValidationError

from mex.backend.main import handle_uncaught_exception
from mex.common.exceptions import MExError


@pytest.mark.parametrize(
    ("exception", "expected"),
    [
        (
            TypeError("foo"),
            {"debug": {"errors": [{"type": "TypeError"}]}, "message": "foo"},
        ),
        (
            ValidationError.from_exception_data(
                "foo",
                [
                    {
                        "type": pydantic_core.PydanticCustomError(
                            "TestError", "You messed up!"
                        ),
                        "loc": ("integerAttribute",),
                        "input": "mumbojumbo",
                    }
                ],
            ),
            {
                "debug": {
                    "errors": [
                        {
                            "input": "mumbojumbo",
                            "loc": ["integerAttribute"],
                            "msg": "You messed up!",
                            "type": "TestError",
                        }
                    ]
                },
                "message": "1 validation error for foo\n"
                "integerAttribute\n"
                "  You messed up! [type=TestError, input_value='mumbojumbo', "
                "input_type=str]",
            },
        ),
        (
            MExError("bar"),
            {"message": "MExError: bar ", "debug": {"errors": [{"type": "MExError"}]}},
        ),
    ],
    ids=["TypeError", "ValidationError", "MExError"],
)
def test_handle_uncaught_exception(
    exception: Exception, expected: dict[str, Any]
) -> None:
    response = handle_uncaught_exception(Mock(), exception)
    assert response.status_code == 500, response.body
    assert json.loads(response.body) == expected
