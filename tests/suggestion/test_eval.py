from types import SimpleNamespace

import pytest
from pytest import CaptureFixture

from mex.backend.suggestion.eval import (
    _as_list,
    _build_report,
    _compute_majority_baseline,
    _concept_uris,
    _echo_report,
    compute_brier_score,
    compute_hit_at_k,
    compute_reciprocal_rank,
    compute_reliability_bins,
)
from mex.common.types import Theme


@pytest.mark.parametrize(
    ("ranked", "actual", "k", "expected"),
    [
        pytest.param(["a", "b"], ["a"], 1, 1.0, id="top hit"),
        pytest.param(["b", "a"], ["a"], 1, 0.0, id="miss at 1"),
        pytest.param(["b", "a"], ["a"], 3, 1.0, id="hit at 3"),
        pytest.param(["b", "c"], ["a"], 3, 0.0, id="miss entirely"),
        pytest.param([], ["a"], 3, 0.0, id="no candidates"),
        pytest.param(["a"], [], 3, 0.0, id="no actual values"),
        pytest.param(["a", "b"], ["b", "z"], 2, 1.0, id="multi valued field"),
    ],
)
def test_compute_hit_at_k(
    ranked: list[str], actual: list[str], k: int, expected: float
) -> None:
    assert compute_hit_at_k(ranked, actual, k) == expected


@pytest.mark.parametrize(
    ("ranked", "actual", "expected"),
    [
        pytest.param(["a", "b"], ["a"], 1.0, id="first"),
        pytest.param(["b", "a"], ["a"], 0.5, id="second"),
        pytest.param(["b", "c", "a"], ["a"], pytest.approx(1 / 3), id="third"),
        pytest.param(["b"], ["a"], 0.0, id="absent"),
    ],
)
def test_compute_reciprocal_rank(
    ranked: list[str], actual: list[str], expected: float
) -> None:
    assert compute_reciprocal_rank(ranked, actual) == expected


def test_compute_reliability_bins() -> None:
    assert compute_reliability_bins(
        [(0.05, False), (0.15, True), (0.15, False), (1.0, True)]
    ) == [
        {"upperEdge": 0.0, "predictions": 1, "observedRate": 0.0},
        {"upperEdge": 0.1, "predictions": 2, "observedRate": 0.5},
        {"upperEdge": 0.9, "predictions": 1, "observedRate": 1.0},
    ]


def test_compute_reliability_bins_empty() -> None:
    assert compute_reliability_bins([]) == []


def test_compute_brier_score() -> None:
    assert compute_brier_score([(1.0, True), (0.0, False)]) == 0.0
    assert compute_brier_score([(1.0, False)]) == 1.0
    assert compute_brier_score([(0.5, True), (0.5, False)]) == 0.25


def test_compute_brier_score_empty() -> None:
    assert compute_brier_score([]) == 0.0


def test_as_list() -> None:
    assert _as_list(None) == []
    assert _as_list("a") == ["a"]
    assert _as_list(["a", "b"]) == ["a", "b"]


def test_concept_uris_unwraps_vocabulary_enums() -> None:
    """`str()` on a vocabulary enum is its member name, not its concept URI.

    Comparing the member name against the URIs the suggestions are keyed by
    would silently score every prediction as a miss.
    """
    assert _concept_uris(Theme["PUBLIC_HEALTH"]) == ["https://mex.rki.de/item/theme-1"]
    assert _concept_uris([Theme["PUBLIC_HEALTH"], Theme["PUBLIC_HEALTH"]]) == [
        "https://mex.rki.de/item/theme-1",
        "https://mex.rki.de/item/theme-1",
    ]
    assert _concept_uris(None) == []


def test_compute_majority_baseline() -> None:
    items = [
        SimpleNamespace(theme=[Theme["PUBLIC_HEALTH"]]),
        SimpleNamespace(theme=[Theme["PUBLIC_HEALTH"]]),
        SimpleNamespace(theme=[Theme["INFECTIOUS_DISEASES_AND_EPIDEMIOLOGY"]]),
    ]

    assert _compute_majority_baseline(items, ["theme"]) == {
        "theme": "https://mex.rki.de/item/theme-1"
    }


def test_compute_majority_baseline_without_values() -> None:
    assert _compute_majority_baseline([SimpleNamespace(theme=None)], ["theme"]) == {}


def test_build_report() -> None:
    report = _build_report(
        hits={"theme": [(1.0, 1.0, 1.0), (0.0, 1.0, 0.5)]},
        baseline_hits={"theme": [1.0, 0.0]},
        observations={"theme": [(0.9, True), (0.4, False)]},
        sampled=4,
    )

    assert report["sampled"] == 4
    field = report["fields"][0]
    assert field["field"] == "theme"
    assert field["coverage"] == 0.5
    assert field["hitAt1"] == 0.5
    assert field["hitAt3"] == 1.0
    assert field["mrr"] == 0.75
    assert field["majorityBaseline"] == 0.5
    assert field["lift"] == 0.0


def test_echo_report(capsys: CaptureFixture[str]) -> None:
    _echo_report(
        {
            "sampled": 1,
            "fields": [
                {
                    "field": "theme",
                    "coverage": 1.0,
                    "hitAt1": 0.8,
                    "hitAt3": 0.9,
                    "mrr": 0.85,
                    "majorityBaseline": 0.4,
                    "lift": 0.4,
                    "brier": 0.1,
                }
            ],
        }
    )

    captured = capsys.readouterr().out
    assert "theme" in captured
    assert "+0.4" in captured
    assert "lift" in captured
