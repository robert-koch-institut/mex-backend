import pytest

from mex.backend.suggestion.models import SuggestionSource
from mex.backend.suggestion.scoring import score_field
from mex.backend.suggestion.transform import FieldAggregation, Neighbour
from mex.backend.types import VocabularyFieldName

THEME = VocabularyFieldName("theme")
ALLOWED = frozenset({"v", "w"})
# prior: v = 100/1000 = 0.1, w = 900/1000 = 0.9
CORPUS = FieldAggregation(field_support=1000, counts={"v": 100, "w": 900})


def _confidences(suggestion: object) -> dict[str, float]:
    return {c.value: c.confidence for c in suggestion.candidates}  # type: ignore[attr-defined]


@pytest.mark.parametrize(
    ("cohort", "expected"),
    [
        # with no neighbourhood the blend is the shrunken cohort estimate alone
        pytest.param(FieldAggregation(2, {"v": 2}), 0.3571, id="2 of 2"),
        pytest.param(FieldAggregation(10, {"v": 10}), 0.7, id="10 of 10"),
        pytest.param(FieldAggregation(200, {"v": 170}), 0.8317, id="170 of 200"),
        pytest.param(FieldAggregation(200, {"v": 0}), 0.0024, id="0 of 200"),
    ],
)
def test_score_field_shrinks_small_cohorts(
    cohort: FieldAggregation, expected: float
) -> None:
    """Pin the worked numbers, so a change to the maths cannot land silently.

    The point of the shrinkage is that a unanimous pair never outranks a strong
    majority, without anyone hard coding a threshold.
    """
    result = score_field(THEME, ALLOWED, cohort, CORPUS, [], 5, 0.0)

    assert _confidences(result)["v"] == expected


def test_score_field_unanimous_pair_ranks_below_strong_majority() -> None:
    pair = score_field(
        THEME, ALLOWED, FieldAggregation(2, {"v": 2}), CORPUS, [], 5, 0.0
    )
    majority = score_field(
        THEME, ALLOWED, FieldAggregation(200, {"v": 170}), CORPUS, [], 5, 0.0
    )

    assert _confidences(pair)["v"] < _confidences(majority)["v"]


def test_score_field_without_any_evidence_returns_the_corpus_prior() -> None:
    result = score_field(THEME, ALLOWED, None, CORPUS, [], 5, 0.0)

    assert _confidences(result) == {"w": 0.9, "v": 0.1}
    assert result.source == SuggestionSource.CORPUS
    assert result.sampleSize == 1000


def test_score_field_without_any_corpus_returns_zero_confidence() -> None:
    result = score_field(THEME, ALLOWED, None, None, [], 5, 0.0)

    assert result.candidates == []
    assert result.sampleSize == 0
    assert result.source == SuggestionSource.CORPUS


def test_score_field_drops_signals_without_evidence_from_the_blend() -> None:
    """A value absent from the cohort must not be propped up by the prior.

    If the empty neighbourhood were shrunk instead of dropped, it would
    contribute the corpus prior at its full blend weight, and `w` would score
    far above the near zero its cohort evidence warrants.
    """
    result = score_field(
        THEME, ALLOWED, FieldAggregation(16, {"v": 16}), CORPUS, [], 5, 0.0
    )

    confidences = _confidences(result)
    assert confidences["v"] > confidences["w"]
    assert confidences["w"] < 0.25


def test_score_field_weights_neighbours_by_relevance() -> None:
    """A more relevant neighbour's vote must count for more.

    Scored against a deliberately uniform prior, so that relevance is the only
    thing separating the two candidates.
    """
    uniform = FieldAggregation(field_support=100, counts={"v": 50, "w": 50})
    neighbours = [
        Neighbour("a", 10.0, {"theme": ["v"]}),
        Neighbour("b", 1.0, {"theme": ["w"]}),
    ]
    result = score_field(THEME, ALLOWED, None, uniform, neighbours, 5, 0.0)

    confidences = _confidences(result)
    assert confidences["v"] > confidences["w"]
    assert result.source == SuggestionSource.NEIGHBOURHOOD
    assert result.sampleSize == 2


def test_score_field_lets_the_prior_dominate_a_thin_neighbourhood() -> None:
    """Two neighbours are not enough to overturn a lopsided corpus prior.

    `w` holds 90% of the corpus, so a pair of neighbours voting for `v` should
    not be able to outrank it. This is the neighbourhood side of the same
    shrinkage that keeps a unanimous pair of cohort items in its place.
    """
    neighbours = [
        Neighbour("a", 2.0, {"theme": ["v"]}),
        Neighbour("b", 2.0, {"theme": ["v"]}),
    ]
    result = score_field(THEME, ALLOWED, None, CORPUS, neighbours, 5, 0.0)

    confidences = _confidences(result)
    assert confidences["v"] == 0.46
    assert confidences["v"] < confidences["w"]


def test_score_field_handles_neighbours_with_zero_scores() -> None:
    """An all-zero-score neighbourhood must weight every neighbour equally."""
    scored = [
        Neighbour("a", 0.0, {"theme": ["v"]}),
        Neighbour("b", 0.0, {"theme": ["v"]}),
    ]
    equal = [
        Neighbour("a", 4.0, {"theme": ["v"]}),
        Neighbour("b", 4.0, {"theme": ["v"]}),
    ]
    result = score_field(THEME, ALLOWED, None, CORPUS, scored, 5, 0.0)

    assert result.sampleSize == 2
    assert _confidences(result) == _confidences(
        score_field(THEME, ALLOWED, None, CORPUS, equal, 5, 0.0)
    )


def test_score_field_skips_neighbours_missing_the_field() -> None:
    neighbours = [
        Neighbour("a", 2.0, {"theme": ["v"]}),
        Neighbour("b", 2.0, {"accessRestriction": ["w"]}),
    ]
    result = score_field(THEME, ALLOWED, None, CORPUS, neighbours, 5, 0.0)

    assert result.sampleSize == 1


def test_score_field_prefers_the_cohort_as_source() -> None:
    result = score_field(
        THEME,
        ALLOWED,
        FieldAggregation(4, {"v": 4}),
        CORPUS,
        [Neighbour("a", 2.0, {"theme": ["w"]})],
        5,
        0.0,
    )

    assert result.source == SuggestionSource.COHORT
    assert result.sampleSize == 4


def test_score_field_ignores_values_outside_the_vocabulary() -> None:
    cohort = FieldAggregation(10, {"v": 8, "stale-value": 9})
    result = score_field(THEME, ALLOWED, cohort, CORPUS, [], 5, 0.0)

    assert "stale-value" not in _confidences(result)


def test_score_field_filters_by_min_confidence() -> None:
    result = score_field(
        THEME, ALLOWED, FieldAggregation(200, {"v": 170}), CORPUS, [], 5, 0.5
    )

    assert list(_confidences(result)) == ["v"]


def test_score_field_can_filter_out_everything() -> None:
    result = score_field(THEME, ALLOWED, None, CORPUS, [], 5, 0.99)

    assert result.candidates == []


def test_score_field_truncates_to_limit() -> None:
    result = score_field(THEME, ALLOWED, None, CORPUS, [], 1, 0.0)

    assert list(_confidences(result)) == ["w"]


def test_score_field_breaks_ties_alphabetically() -> None:
    corpus = FieldAggregation(field_support=2, counts={"v": 1, "w": 1})
    result = score_field(THEME, ALLOWED, None, corpus, [], 5, 0.0)

    assert [c.value for c in result.candidates] == ["v", "w"]


def test_score_field_falls_back_to_corpus_support() -> None:
    result = score_field(THEME, ALLOWED, None, CORPUS, [], 5, 0.0)

    assert {c.value: c.support for c in result.candidates} == {"w": 900, "v": 100}


def test_score_field_counts_support_over_one_population() -> None:
    """Every candidate's support must be counted over the same population.

    Mixing them makes the numbers incomparable: a value counted over the whole
    corpus sits next to one counted over a small cohort and looks far better
    evidenced purely because the corpus is bigger. Here `w` holds 900 corpus
    items but none in the cohort, so against a cohort it must report 0, and
    `support` over `sampleSize` must read as a ratio.
    """
    cohort = FieldAggregation(field_support=16, counts={"v": 12})

    result = score_field(THEME, ALLOWED, cohort, CORPUS, [], 5, 0.0)

    supports = {c.value: c.support for c in result.candidates}
    assert supports == {"v": 12, "w": 0}
    assert result.sampleSize == 16
    assert all(c.support <= result.sampleSize for c in result.candidates)


def test_score_field_counts_support_over_the_neighbourhood() -> None:
    neighbours = [
        Neighbour("a", 4.0, {"theme": ["v"]}),
        Neighbour("b", 4.0, {"theme": ["v"]}),
        Neighbour("c", 4.0, {"theme": ["w"]}),
    ]

    result = score_field(THEME, ALLOWED, None, CORPUS, neighbours, 5, 0.0)

    assert {c.value: c.support for c in result.candidates} == {"v": 2, "w": 1}
    assert result.sampleSize == 3
