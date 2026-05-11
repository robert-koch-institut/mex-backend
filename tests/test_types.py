import secrets
from enum import Enum

from mex.backend.models import APIKeyDatabase
from mex.backend.types import APIKey, DynamicStrEnum


def test_api_key_database() -> None:
    data_dict = {"read": ["foo"], "write": ["bar", "batz"]}
    db = APIKeyDatabase(**data_dict)

    assert db.read == [APIKey("foo")]
    assert db.write == [APIKey("bar"), APIKey("batz")]


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
