import secrets
from enum import Enum

from mex.backend.types import (
    APIKey,
    APIKeyDatabase,
    DynamicStrEnum,
)


def test_api_key_database() -> None:
    data_dict = {"read": "foo", "write": "bar"}
    db = APIKeyDatabase(**data_dict)

    assert db.read == [APIKey("foo")]


def test_api_key_repr() -> None:
    key = APIKey(secrets.token_urlsafe())
    key_repr = repr(key)
    assert key_repr == "APIKey('**********')"


def test_dynamic_str_enum() -> None:
    class Dummy(Enum, metaclass=DynamicStrEnum):
        __names__ = ["foo", "Bar", "LoremIpsum"]

    assert {d.name: str(d.value) for d in Dummy} == {
        "FOO": "foo",
        "BAR": "Bar",
        "LOREM_IPSUM": "LoremIpsum",
    }
