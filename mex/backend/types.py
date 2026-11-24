from enum import Enum, EnumMeta, _EnumDict
from itertools import chain

from pydantic import SecretStr

from mex.common.fields import (
    ALL_REFERENCE_FIELD_NAMES,
    VOCABULARY_FIELDS_BY_CLASS_NAME,
)
from mex.common.models import (
    ADDITIVE_MODEL_CLASSES_BY_NAME,
    EXTRACTED_MODEL_CLASSES_BY_NAME,
    MERGED_MODEL_CLASSES_BY_NAME,
    RULE_MODEL_CLASSES_BY_NAME,
)
from mex.common.transform import dromedary_to_snake

# fields from any extracted or additive class that contain vocabulary values
ALL_VOCABULARY_FIELD_NAMES = sorted(
    {
        field_name
        for class_name in chain(
            EXTRACTED_MODEL_CLASSES_BY_NAME,
            ADDITIVE_MODEL_CLASSES_BY_NAME,
        )
        for field_name in VOCABULARY_FIELDS_BY_CLASS_NAME[class_name]
    }
)


class AccessLevel(Enum):
    """Enum of access level."""

    READ = "read"
    WRITE = "write"


class CacheConnectorType(Enum):
    """Choice of available cache connectors."""

    MEMORY = "memory"
    VALKEY = "valkey"


class APIKey(SecretStr):
    """An API Key used for authenticating and authorizing a client."""

    def __repr__(self) -> str:
        """Return a secure representation of this key."""
        return f"APIKey('{self}')"


class DynamicStrEnum(EnumMeta):
    """Metaclass to dynamically populate an enumeration from a list of strings."""

    def __new__(
        cls: type[DynamicStrEnum], name: str, bases: tuple[type], dct: _EnumDict
    ) -> DynamicStrEnum:
        """Create a new enum by adding an entry for each name in the source."""
        for value in dct.pop("__names__"):
            dct[dromedary_to_snake(value).upper()] = value
        return super().__new__(cls, name, bases, dct)


class ExtractedType(Enum, metaclass=DynamicStrEnum):
    """Enumeration of possible types for extracted items."""

    __names__ = list(EXTRACTED_MODEL_CLASSES_BY_NAME)


class MergedType(Enum, metaclass=DynamicStrEnum):
    """Enumeration of possible types for merged items."""

    __names__ = list(MERGED_MODEL_CLASSES_BY_NAME)


class RuleType(Enum, metaclass=DynamicStrEnum):
    """Enumeration of possible types for rule items."""

    __names__ = list(RULE_MODEL_CLASSES_BY_NAME)


class ReferenceFieldName(Enum, metaclass=DynamicStrEnum):
    """Enumeration of possible field names that contain references."""

    __names__ = sorted(ALL_REFERENCE_FIELD_NAMES)


class VocabularyFieldName(Enum, metaclass=DynamicStrEnum):
    """Enumeration of possible field names that contain vocabulary values."""

    __names__ = ALL_VOCABULARY_FIELD_NAMES
