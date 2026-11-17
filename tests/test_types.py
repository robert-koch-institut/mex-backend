import secrets
from enum import Enum

from mex.backend.models import APIKeyDatabase
from mex.backend.types import (
    ALL_VOCABULARY_FIELD_NAMES,
    APIKey,
    DynamicStrEnum,
    VocabularyFieldName,
)
from mex.common.fields import VOCABULARY_FIELDS_BY_CLASS_NAME
from mex.common.models import MERGED_MODEL_CLASSES_BY_NAME


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


def test_vocabulary_field_name() -> None:
    assert {str(v.value) for v in VocabularyFieldName} == set(
        ALL_VOCABULARY_FIELD_NAMES
    )
    assert VocabularyFieldName("theme").name == "THEME"
    assert VocabularyFieldName("accessRestriction").name == "ACCESS_RESTRICTION"


def test_vocabulary_fields_agree_across_model_variants() -> None:
    """Assert the merged class is a safe proxy for its extracted and additive kin.

    The suggestion endpoint derives the vocabulary fields to aggregate from the
    merged class name, but aggregates over extracted, additive and subtractive
    nodes. That is only sound while all variants of a stem type declare the same
    vocabulary fields, so pin the invariant here rather than discover a mex-common
    bump broke it via a silently empty suggestion response.
    """
    for class_name in MERGED_MODEL_CLASSES_BY_NAME:
        stem_type = class_name.removeprefix("Merged")
        merged_fields = VOCABULARY_FIELDS_BY_CLASS_NAME[class_name]
        for prefix in ("Extracted", "Additive", "Subtractive"):
            assert (
                VOCABULARY_FIELDS_BY_CLASS_NAME[f"{prefix}{stem_type}"] == merged_fields
            ), f"{prefix}{stem_type} disagrees with {class_name}"
