import pytest

from mex.backend.suggestion.transform import (
    FieldAggregation,
    Neighbour,
    escape_query_string,
    transform_aggregations,
    transform_neighbours,
)


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        pytest.param("Kohortenstudie", "Kohortenstudie", id="plain"),
        pytest.param("Studie AND", "Studie", id="trailing boolean operator"),
        pytest.param("half-typed (", r"half\-typed \(", id="unbalanced paren"),
        pytest.param("C++ / R", r"C\+\+ \/ R", id="slashes and plusses"),
        pytest.param('say "hi"', r"say \"hi\"", id="quotes"),
        pytest.param("50% [draft]", r"50% \[draft\]", id="brackets"),
        pytest.param("Müller-Lüdenscheidt", r"Müller\-Lüdenscheidt", id="umlauts"),
        pytest.param("a:b~2^3", r"a\:b\~2\^3", id="lucene operators"),
        pytest.param("foo && bar || baz", r"foo \&& bar \|| baz", id="boolean symbols"),
        pytest.param("NOT", "", id="only a reserved word"),
        pytest.param("   ", "", id="only whitespace"),
        pytest.param("", "", id="empty"),
    ],
)
def test_escape_query_string(raw: str, expected: str) -> None:
    assert escape_query_string(raw) == expected


def test_transform_aggregations() -> None:
    assert transform_aggregations(
        [
            {
                "field": "theme",
                "fieldSupport": 16,
                "candidates": [
                    {"value": "https://mex.rki.de/item/theme-1", "support": 12},
                    {"value": "https://mex.rki.de/item/theme-11", "support": 3},
                ],
            }
        ]
    ) == {
        "theme": FieldAggregation(
            field_support=16,
            counts={
                "https://mex.rki.de/item/theme-1": 12,
                "https://mex.rki.de/item/theme-11": 3,
            },
        )
    }


def test_transform_aggregations_empty() -> None:
    assert transform_aggregations([]) == {}


def test_transform_neighbours() -> None:
    assert transform_neighbours(
        [
            {
                "identifier": "bFQoRhcVH5DHUx",
                "score": 2.5,
                "fields": [
                    {
                        "field": "theme",
                        "values": ["https://mex.rki.de/item/theme-1"],
                    }
                ],
            }
        ]
    ) == [
        Neighbour(
            identifier="bFQoRhcVH5DHUx",
            score=2.5,
            values_by_field={"theme": ["https://mex.rki.de/item/theme-1"]},
        )
    ]


def test_transform_neighbours_empty() -> None:
    assert transform_neighbours([]) == []
