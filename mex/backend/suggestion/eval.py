"""Offline evaluation of vocabulary suggestions against a real graph.

Runs leave-one-out over the items already in whatever graph `MEX_GRAPH_URL`
points at: for each sampled item, rebuild the draft a user would have typed
from the item's own text and references, ask for suggestions with that item
excluded, and check whether its actual values come back.

What this can establish, and what it cannot, depends entirely on the graph it
is pointed at. On artificial data, where vocabulary values are drawn at random,
hit rates are uninformative by construction, but calibration and lift are not:
well calibrated confidences on noise validate the shrinkage maths, and lift
over the majority baseline must come out near zero. Anything else means the
scorer is fitting graph structure rather than content. Real accuracy numbers
require a graph with real data.
"""

import json
import random
from collections import defaultdict
from typing import TYPE_CHECKING, Any

import click

from mex.backend.merged.helpers import search_merged_items_in_graph
from mex.backend.settings import BackendSettings
from mex.backend.suggestion.helpers import suggest_vocabulary_values_from_graph
from mex.common.fields import (
    REFERENCE_FIELDS_BY_CLASS_NAME,
    TEXT_FIELDS_BY_CLASS_NAME,
    VOCABULARY_FIELDS_BY_CLASS_NAME,
)
from mex.common.models import ADDITIVE_MODEL_CLASSES_BY_NAME
from mex.common.transform import ensure_prefix
from mex.common.types import Validation

if TYPE_CHECKING:
    from collections.abc import Sequence

#: the search endpoint caps `limit` at 100 per call
PAGE_SIZE = 100

#: upper edge of each reliability bin
RELIABILITY_BINS = [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.01]


def compute_hit_at_k(ranked: Sequence[str], actual: Sequence[str], k: int) -> float:
    """Return 1.0 when any actual value appears in the top `k` candidates."""
    if not actual:
        return 0.0
    return float(bool(set(ranked[:k]) & set(actual)))


def compute_reciprocal_rank(ranked: Sequence[str], actual: Sequence[str]) -> float:
    """Return the reciprocal of the best rank holding an actual value."""
    for position, value in enumerate(ranked, start=1):
        if value in actual:
            return 1.0 / position
    return 0.0


def compute_reliability_bins(
    observations: Sequence[tuple[float, bool]],
) -> list[dict[str, float]]:
    """Bucket predictions by confidence and report the observed hit rate.

    This is the number that decides whether a client can threshold on
    confidence at all: if the endpoint says 0.8, it had better be right about
    80% of the time.
    """
    buckets: defaultdict[float, list[bool]] = defaultdict(list)
    for confidence, was_correct in observations:
        edge = next(b for b in RELIABILITY_BINS if confidence < b)
        buckets[edge].append(was_correct)
    return [
        {
            "upperEdge": round(edge - 0.1, 1),
            "predictions": len(hits),
            "observedRate": round(sum(hits) / len(hits), 4),
        }
        for edge, hits in sorted(buckets.items())
        if hits
    ]


def compute_brier_score(observations: Sequence[tuple[float, bool]]) -> float:
    """Return the mean squared error between confidence and outcome."""
    if not observations:
        return 0.0
    return round(
        sum((confidence - was_correct) ** 2 for confidence, was_correct in observations)
        / len(observations),
        4,
    )


@click.command()
@click.option("--entity-type", default="MergedResource", type=str)
@click.option("--sample", default=200, type=int)
@click.option("--seed", default=1, type=int)
@click.option("--blend-weight", default=None, type=float)
@click.option("--json-out", "json_out", default=None, type=str)
def main(  # pragma: no cover
    entity_type: str,
    sample: int,
    seed: int,
    blend_weight: float | None,
    json_out: str | None,
) -> None:
    """Evaluate vocabulary suggestions by leave-one-out over an existing graph.

    Args:
        entity_type: Merged entity type to evaluate, e.g. MergedResource.
        sample: Maximum number of items to evaluate.
        seed: Random seed for sampling.
        blend_weight: Override the cohort blend weight, for parameter sweeps.
        json_out: Write the full report to this path as JSON.
    """
    settings = BackendSettings.get()
    if blend_weight is not None:
        settings.suggestion_cohort_blend_weight = blend_weight

    stem_type = entity_type.removeprefix("Merged")
    additive_class = ADDITIVE_MODEL_CLASSES_BY_NAME[
        ensure_prefix(stem_type, "Additive")
    ]
    vocabulary_fields = VOCABULARY_FIELDS_BY_CLASS_NAME[entity_type]
    text_fields = TEXT_FIELDS_BY_CLASS_NAME[entity_type]
    reference_fields = REFERENCE_FIELDS_BY_CLASS_NAME[
        ensure_prefix(stem_type, "Additive")
    ]
    if not vocabulary_fields:
        click.echo(f"{entity_type} has no vocabulary fields, nothing to evaluate")
        return

    items = _fetch_items(entity_type, sample, seed)
    if not items:
        click.echo(f"no {entity_type} items in the graph, nothing to evaluate")
        return
    click.echo(f"evaluating {len(items)} {entity_type} items")

    hits: defaultdict[str, list[tuple[float, float, float]]] = defaultdict(list)
    observations: defaultdict[str, list[tuple[float, bool]]] = defaultdict(list)
    baseline_hits: defaultdict[str, list[float]] = defaultdict(list)
    majority = _compute_majority_baseline(items, vocabulary_fields)

    for item in items:
        draft = additive_class.model_validate(
            {
                field: getattr(item, field)
                for field in (*text_fields, *reference_fields)
                if getattr(item, field, None)
            }
        )
        response = suggest_vocabulary_values_from_graph(
            additive=draft,
            requested_fields=[],
            exclude_identifier=str(item.identifier),
            limit=10,
            min_confidence=0.0,
        )
        by_field = {str(s.field.value): s for s in response.suggestions}
        for field in vocabulary_fields:
            actual = _concept_uris(getattr(item, field, None))
            if not actual:
                continue
            suggestion = by_field.get(field)
            ranked = [c.value for c in suggestion.candidates] if suggestion else []
            hits[field].append(
                (
                    compute_hit_at_k(ranked, actual, 1),
                    compute_hit_at_k(ranked, actual, 3),
                    compute_reciprocal_rank(ranked, actual),
                )
            )
            baseline_hits[field].append(float(majority.get(field) in actual))
            if suggestion and suggestion.candidates:
                top = suggestion.candidates[0]
                observations[field].append((top.confidence, top.value in actual))

    report = _build_report(hits, baseline_hits, observations, len(items))
    _echo_report(report)
    if json_out:
        with open(json_out, "w") as handle:  # noqa: PTH123
            json.dump(report, handle, indent=2)
        click.echo(f"wrote {json_out}")


def _fetch_items(entity_type: str, sample: int, seed: int) -> list[Any]:
    """Page through the graph and return a shuffled sample of merged items.

    The search endpoint caps `limit` at 100 per call, so a sample larger than
    that has to be paged rather than asked for in one go.
    """
    items: list[Any] = []
    skip = 0
    while True:
        container = search_merged_items_in_graph(
            entity_type=[entity_type],
            skip=skip,
            limit=PAGE_SIZE,
            validation=Validation.IGNORE,
        )
        items.extend(container.items)
        skip += PAGE_SIZE
        if skip >= container.total or not container.items:
            break
    random.Random(seed).shuffle(items)  # noqa: S311 - sampling, not crypto
    return items[:sample]


def _as_list(value: Any) -> list[Any]:  # noqa: ANN401
    """Normalize a scalar, list or None field value into a list."""
    if value is None:
        return []
    if isinstance(value, list):
        return value
    return [value]


def _concept_uris(value: Any) -> list[str]:  # noqa: ANN401
    """Return the concept URIs of a vocabulary field value.

    Vocabulary values are enum members whose `str()` is the member name, not
    the URI the suggestions are keyed by, so unwrapping `.value` is required
    for the comparison to mean anything.
    """
    return [str(getattr(item, "value", item)) for item in _as_list(value)]


def _compute_majority_baseline(
    items: Sequence[Any], vocabulary_fields: Sequence[str]
) -> dict[str, str]:
    """Return the single most common value per field, the baseline to beat."""
    counts: defaultdict[str, defaultdict[str, int]] = defaultdict(
        lambda: defaultdict(int)
    )
    for item in items:
        for field in vocabulary_fields:
            for value in _concept_uris(getattr(item, field, None)):
                counts[field][value] += 1
    return {
        field: max(values, key=lambda v: values[v])
        for field, values in counts.items()
        if values
    }


def _build_report(
    hits: dict[str, list[tuple[float, float, float]]],
    baseline_hits: dict[str, list[float]],
    observations: dict[str, list[tuple[float, bool]]],
    sampled: int,
) -> dict[str, Any]:
    """Assemble per field metrics into a serializable report."""
    fields = []
    for field, scores in sorted(hits.items()):
        hit_1 = sum(s[0] for s in scores) / len(scores)
        baseline = sum(baseline_hits[field]) / len(baseline_hits[field])
        fields.append(
            {
                "field": field,
                "coverage": round(len(scores) / sampled, 4),
                "hitAt1": round(hit_1, 4),
                "hitAt3": round(sum(s[1] for s in scores) / len(scores), 4),
                "mrr": round(sum(s[2] for s in scores) / len(scores), 4),
                "majorityBaseline": round(baseline, 4),
                "lift": round(hit_1 - baseline, 4),
                "brier": compute_brier_score(observations.get(field, [])),
                "reliability": compute_reliability_bins(observations.get(field, [])),
            }
        )
    return {"sampled": sampled, "fields": fields}


def _echo_report(report: dict[str, Any]) -> None:
    """Print the report as a table, lift last because it is the decision."""
    header = (
        f"{'field':32} {'cov':>6} {'hit@1':>6} {'hit@3':>6} "
        f"{'mrr':>6} {'base':>6} {'lift':>7} {'brier':>6}"
    )
    click.echo(header)
    click.echo("-" * len(header))
    for field in report["fields"]:
        click.echo(
            f"{field['field']:32} {field['coverage']:>6} {field['hitAt1']:>6} "
            f"{field['hitAt3']:>6} {field['mrr']:>6} {field['majorityBaseline']:>6} "
            f"{field['lift']:>+7} {field['brier']:>6}"
        )
    click.echo(
        "\nlift is hit@1 minus the always-guess-the-most-common baseline: "
        "it is the only column that says whether this is worth shipping"
    )
