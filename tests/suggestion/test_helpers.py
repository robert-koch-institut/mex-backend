import time
from typing import TYPE_CHECKING

import pytest
from neo4j.exceptions import Neo4jError
from pytest import MonkeyPatch

from mex.backend.cache import get_cache_connector
from mex.backend.graph.connector import GraphConnector
from mex.backend.settings import BackendSettings
from mex.backend.suggestion.constants import CORPUS_PRIOR_CACHE_PREFIX
from mex.backend.suggestion.helpers import (
    CachedAggregations,
    derive_field_names,
    derive_query_string,
    derive_reference_filters,
    get_corpus_prior,
    suggest_vocabulary_values_from_graph,
)
from mex.backend.suggestion.models import SuggestionSource
from mex.backend.types import VocabularyFieldName
from mex.common.models import AdditiveContactPoint, AdditiveResource
from mex.common.types import Text, TextLanguage, Theme

if TYPE_CHECKING:
    from tests.conftest import MockedGraph

EMPTY_AGGREGATION: list[dict[str, object]] = [{"cohortSize": 0, "aggregations": []}]
EMPTY_NEIGHBOURHOOD: list[dict[str, object]] = [{"neighbours": []}]


def test_derive_query_string_joins_text_and_string_fields() -> None:
    additive = AdditiveResource(
        title=[Text(value="Kohortenstudie", language=TextLanguage.DE)],
        description=[Text(value="Eine Studie", language=TextLanguage.DE)],
        doi="https://doi.org/10.1000/xyz",
    )

    query_string = derive_query_string(additive)

    assert "Kohortenstudie" in query_string
    assert "Eine Studie" in query_string
    assert r"\/" in query_string  # the doi's slashes are escaped


def test_derive_query_string_is_empty_without_text() -> None:
    assert derive_query_string(AdditiveResource()) == ""


def test_derive_query_string_escapes_half_typed_input() -> None:
    additive = AdditiveResource(
        title=[Text(value="Laborbefund AND (", language=TextLanguage.DE)]
    )

    assert derive_query_string(additive) == r"Laborbefund \("


def test_derive_reference_filters() -> None:
    additive = AdditiveResource(unitInCharge=["bFQoRhcVH5DHUq"])

    filters = derive_reference_filters(additive)

    assert len(filters) == 1
    assert str(filters[0].field.value) == "unitInCharge"
    assert filters[0].identifiers == ["bFQoRhcVH5DHUq"]


def test_derive_reference_filters_without_references() -> None:
    assert derive_reference_filters(AdditiveResource()) == []


def test_derive_field_names_defaults_to_unfilled_fields() -> None:
    additive = AdditiveResource(theme=[Theme["PUBLIC_HEALTH"]])

    field_names = derive_field_names(additive, [])

    assert "theme" not in field_names
    assert "accessRestriction" in field_names


def test_derive_field_names_honours_an_explicit_request() -> None:
    additive = AdditiveResource(theme=[Theme["PUBLIC_HEALTH"]])

    assert derive_field_names(additive, [VocabularyFieldName("theme")]) == ["theme"]


def test_derive_field_names_drops_fields_foreign_to_the_entity_type() -> None:
    assert (
        derive_field_names(AdditiveResource(), [VocabularyFieldName("activityType")])
        == []
    )


@pytest.mark.usefixtures("settings")
def test_get_corpus_prior_caches_the_result(mocked_graph: MockedGraph) -> None:
    mocked_graph.return_value = EMPTY_AGGREGATION

    get_corpus_prior("MergedResource", ["theme"])

    assert len(mocked_graph.call_args_list) == 1
    assert get_cache_connector().get_value(
        f"{CORPUS_PRIOR_CACHE_PREFIX}:MergedResource"
    )

    get_corpus_prior("MergedResource", ["theme"])

    assert len(mocked_graph.call_args_list) == 1  # served from the cache


@pytest.mark.usefixtures("settings")
def test_get_corpus_prior_refetches_a_stale_entry(mocked_graph: MockedGraph) -> None:
    get_cache_connector().set_value(
        f"{CORPUS_PRIOR_CACHE_PREFIX}:MergedResource",
        CachedAggregations(
            createdAt=time.time() - 10_000, cohortSize=1, aggregations=[]
        ),
    )
    mocked_graph.return_value = EMPTY_AGGREGATION

    get_corpus_prior("MergedResource", ["theme"])

    assert len(mocked_graph.call_args_list) == 1


@pytest.mark.usefixtures("settings")
def test_suggest_returns_early_without_vocabulary_fields(
    mocked_graph: MockedGraph,
) -> None:
    """Six of fourteen merged types have no vocabulary fields at all."""
    response = suggest_vocabulary_values_from_graph(
        AdditiveContactPoint(), [], None, 3, 0.0
    )

    assert response.suggestions == []
    assert mocked_graph.call_args_list == []


@pytest.mark.usefixtures("settings")
def test_suggest_skips_the_neighbourhood_without_text(
    mocked_graph: MockedGraph,
) -> None:
    mocked_graph.return_value = EMPTY_AGGREGATION

    suggest_vocabulary_values_from_graph(
        AdditiveResource(unitInCharge=["bFQoRhcVH5DHUq"]), [], None, 3, 0.0
    )

    # the corpus prior and the cohort, but no neighbourhood query
    assert len(mocked_graph.call_args_list) == 2


@pytest.mark.usefixtures("settings")
def test_suggest_skips_the_cohort_without_references(
    mocked_graph: MockedGraph,
) -> None:
    mocked_graph.side_effect = [EMPTY_AGGREGATION, EMPTY_NEIGHBOURHOOD]

    suggest_vocabulary_values_from_graph(
        AdditiveResource(title=[Text(value="Studie", language=TextLanguage.DE)]),
        [],
        None,
        3,
        0.0,
    )

    # the corpus prior and the neighbourhood, but no cohort query
    assert len(mocked_graph.call_args_list) == 2


@pytest.mark.usefixtures("settings")
def test_suggest_skips_the_neighbourhood_when_text_escapes_to_nothing(
    mocked_graph: MockedGraph,
) -> None:
    mocked_graph.return_value = EMPTY_AGGREGATION

    suggest_vocabulary_values_from_graph(
        AdditiveResource(title=[Text(value="NOT", language=TextLanguage.DE)]),
        [],
        None,
        3,
        0.0,
    )

    assert len(mocked_graph.call_args_list) == 1


@pytest.mark.usefixtures("settings")
def test_suggest_degrades_when_a_signal_fails(
    mocked_graph: MockedGraph, monkeypatch: MonkeyPatch
) -> None:
    """A failing signal must not take the whole response down.

    The endpoint is called while a user types, so a slightly worse answer is
    strictly better than a 500 landing in a live form.
    """
    mocked_graph.return_value = EMPTY_AGGREGATION
    message = "neighbourhood is having a bad day"

    def explode(*_args: object, **_kwargs: object) -> None:
        raise Neo4jError(message)

    monkeypatch.setattr(GraphConnector, "fetch_vocabulary_neighbourhood", explode)

    response = suggest_vocabulary_values_from_graph(
        AdditiveResource(title=[Text(value="Studie", language=TextLanguage.DE)]),
        [],
        None,
        3,
        0.0,
    )

    assert response.neighbourhoodSize == 0
    assert response.suggestions  # the corpus prior still answered


@pytest.mark.usefixtures("settings")
def test_suggest_degrades_when_the_cohort_fails(
    mocked_graph: MockedGraph, monkeypatch: MonkeyPatch
) -> None:
    mocked_graph.return_value = EMPTY_AGGREGATION
    original = GraphConnector.aggregate_vocabulary_values
    calls = {"n": 0}
    message = "cohort is having a bad day"

    def explode_on_cohort(self: GraphConnector, **kwargs: object) -> object:
        calls["n"] += 1
        if calls["n"] > 1:  # the first call is the corpus prior
            raise Neo4jError(message)
        return original(self, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(
        GraphConnector, "aggregate_vocabulary_values", explode_on_cohort
    )

    response = suggest_vocabulary_values_from_graph(
        AdditiveResource(unitInCharge=["bFQoRhcVH5DHUq"]), [], None, 3, 0.0
    )

    assert all(s.source == SuggestionSource.CORPUS for s in response.suggestions)


@pytest.mark.usefixtures("settings")
def test_suggest_respects_the_configured_ttl(
    mocked_graph: MockedGraph, monkeypatch: MonkeyPatch
) -> None:
    settings = BackendSettings.get()
    monkeypatch.setattr(settings, "suggestion_corpus_prior_ttl", 1)
    get_cache_connector().set_value(
        f"{CORPUS_PRIOR_CACHE_PREFIX}:MergedResource",
        CachedAggregations(createdAt=time.time(), cohortSize=7, aggregations=[]),
    )
    mocked_graph.return_value = EMPTY_AGGREGATION

    cohort_size, _ = get_corpus_prior("MergedResource", ["theme"])

    assert cohort_size == 7
    assert mocked_graph.call_args_list == []
