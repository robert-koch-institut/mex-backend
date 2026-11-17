from collections import Counter, defaultdict
from typing import TYPE_CHECKING

from mex.backend.settings import BackendSettings
from mex.backend.suggestion.models import (
    FieldSuggestion,
    SuggestionSource,
    VocabularyCandidate,
)

if TYPE_CHECKING:
    from collections.abc import Sequence

    from mex.backend.suggestion.transform import FieldAggregation, Neighbour
    from mex.backend.types import VocabularyFieldName


def score_field(  # noqa: PLR0913, PLR0917
    field_name: VocabularyFieldName,
    allowed_values: frozenset[str],
    cohort: FieldAggregation | None,
    corpus: FieldAggregation | None,
    neighbours: Sequence[Neighbour],
    limit: int,
    min_confidence: float,
) -> FieldSuggestion:
    """Blend the cohort, neighbourhood and corpus signals for one field.

    Both conditioned signals are shrunk toward the corpus prior with a pseudo
    count, so a thin cohort or a thin neighbourhood degrades smoothly into the
    corpus prior instead of falling off a threshold. Because both pseudo counts
    are constrained to be greater than zero, every denominator is non-zero by
    construction.

    A signal with no evidence whatsoever is dropped from the blend rather than
    shrunk, and the remaining weights are renormalised. Shrinking it would let
    it contribute the corpus prior at full weight, which quietly dilutes a
    strong signal: a value with no items at all in the cohort would still score
    well above zero purely because the user has not typed any text yet. If
    neither signal has evidence the result is the corpus prior itself.

    Neighbour weights are normalised by the top score, because raw lucene scores
    are unbounded and scale with query length, so an absolute scale would make
    the blend weight mean different things for a short and a long draft.

    The blend of two probabilities is itself a probability, so the result needs
    no further normalisation. For list valued fields the confidences across
    values do not sum to one, which is correct: the quantity is the fraction of
    comparable items that include the value, not a distribution over values.

    Args:
        field_name: The vocabulary field being suggested
        allowed_values: Values the vocabulary permits for this field
        cohort: Distribution over items sharing the draft's references, if any
        corpus: Distribution over all items of the entity type, if any
        neighbours: Top scoring full text matches, may be empty
        limit: How many candidates to return
        min_confidence: Drop candidates scoring below this

    Returns:
        Ranked candidates with confidences, plus the evidence behind them
    """
    settings = BackendSettings.get()
    alpha = settings.suggestion_cohort_blend_weight
    cohort_pseudo = settings.suggestion_cohort_pseudo_count
    neighbour_pseudo = settings.suggestion_neighbourhood_pseudo_weight

    corpus_support = corpus.field_support if corpus else 0
    corpus_counts = corpus.counts if corpus else {}
    cohort_support = cohort.field_support if cohort else 0
    cohort_counts = cohort.counts if cohort else {}

    key = str(field_name.value)
    top_score = max((n.score for n in neighbours), default=0.0)
    neighbour_weights: defaultdict[str, float] = defaultdict(float)
    neighbour_counts: Counter[str] = Counter()
    neighbour_total = 0.0
    neighbour_items = 0
    for neighbour in neighbours:
        if not (values := neighbour.values_by_field.get(key)):
            continue
        weight = neighbour.score / top_score if top_score > 0 else 1.0
        neighbour_total += weight
        neighbour_items += 1
        for value in values:
            neighbour_weights[value] += weight
            neighbour_counts[value] += 1

    cohort_weight = alpha if cohort_support else 0.0
    neighbour_weight = (1.0 - alpha) if neighbour_items else 0.0
    total_weight = cohort_weight + neighbour_weight

    # support must count the same population that sampleSize denominates, or the
    # two cannot be read as a ratio and candidates cannot be compared with each
    # other: a value counted over the corpus sitting next to one counted over the
    # cohort looks far better evidenced purely because the corpus is bigger
    if cohort_support:
        source = SuggestionSource.COHORT
        support_counts: dict[str, int] = cohort_counts
        sample_size = cohort_support
    elif neighbour_items:
        source = SuggestionSource.NEIGHBOURHOOD
        support_counts = dict(neighbour_counts)
        sample_size = neighbour_items
    else:
        source = SuggestionSource.CORPUS
        support_counts = corpus_counts
        sample_size = corpus_support

    candidates: list[VocabularyCandidate] = []
    observed = set(cohort_counts) | set(neighbour_counts) | set(corpus_counts)
    for value in sorted(observed & allowed_values):
        prior = corpus_counts.get(value, 0) / corpus_support if corpus_support else 0.0
        if not total_weight:
            confidence = prior
        else:
            cohort_estimate = (cohort_counts.get(value, 0) + cohort_pseudo * prior) / (
                cohort_support + cohort_pseudo
            )
            neighbour_estimate = (
                neighbour_weights.get(value, 0.0) + neighbour_pseudo * prior
            ) / (neighbour_total + neighbour_pseudo)
            confidence = (
                cohort_weight * cohort_estimate + neighbour_weight * neighbour_estimate
            ) / total_weight
        if confidence < min_confidence:
            continue
        candidates.append(
            VocabularyCandidate(
                value=value,
                confidence=round(confidence, 4),
                support=support_counts.get(value, 0),
            )
        )
    candidates.sort(key=lambda candidate: (-candidate.confidence, candidate.value))

    return FieldSuggestion(
        field=field_name,
        candidates=candidates[:limit],
        sampleSize=sample_size,
        source=source,
    )
