import re
from dataclasses import dataclass
from typing import Any, Final

LUCENE_SPECIAL_CHARACTERS: Final = re.compile(r'(&&|\|\||[+\-!(){}\[\]^"~*?:\\/])')
LUCENE_RESERVED_WORDS: Final = frozenset({"AND", "OR", "NOT", "TO"})


@dataclass(frozen=True, slots=True)
class FieldAggregation:
    """Distribution of one vocabulary field's values over a set of merged items."""

    field_support: int
    counts: dict[str, int]


@dataclass(frozen=True, slots=True)
class Neighbour:
    """A merged item found by full text search, with its relevance score."""

    identifier: str
    score: float
    values_by_field: dict[str, list[str]]


def escape_query_string(value: str) -> str:
    """Turn arbitrary in-progress user input into a safe lucene query string.

    The full text index is queried through `db.index.fulltext.queryNodes`, which
    parses its argument as a lucene query. Half typed input such as "Studie AND"
    or "half-typed (" raises a lucene ParseException, which surfaces as a 500.
    Suggestions are requested on every debounced keystroke, so that is the
    common case rather than a rare one.

    Each whitespace separated token is escaped on its own and bare boolean
    operators are dropped, which keeps the default or-of-terms behaviour that a
    typeahead wants instead of degrading into a strict phrase query.

    Args:
        value: Raw text from the draft item, possibly mid-word or unbalanced

    Returns:
        An escaped query string, or an empty string if nothing searchable is left
    """
    return " ".join(
        LUCENE_SPECIAL_CHARACTERS.sub(r"\\\1", token)
        for token in value.split()
        if token not in LUCENE_RESERVED_WORDS
    )


def transform_aggregations(
    raw_aggregations: list[dict[str, Any]],
) -> dict[str, FieldAggregation]:
    """Transform raw aggregation records into a lookup by field name."""
    return {
        str(raw["field"]): FieldAggregation(
            field_support=int(raw["fieldSupport"]),
            counts={
                str(candidate["value"]): int(candidate["support"])
                for candidate in raw["candidates"]
            },
        )
        for raw in raw_aggregations
    }


def transform_neighbours(raw_neighbours: list[dict[str, Any]]) -> list[Neighbour]:
    """Transform raw neighbourhood records into neighbour instances."""
    return [
        Neighbour(
            identifier=str(raw["identifier"]),
            score=float(raw["score"]),
            values_by_field={
                str(field["field"]): [str(value) for value in field["values"]]
                for field in raw["fields"]
            },
        )
        for raw in raw_neighbours
    ]
