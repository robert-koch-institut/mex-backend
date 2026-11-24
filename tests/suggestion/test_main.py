from typing import TYPE_CHECKING, Any

import pytest

from mex.backend.suggestion.models import SuggestionSource

if TYPE_CHECKING:
    from fastapi.testclient import TestClient

    from tests.conftest import MockedGraph
    from tests.suggestion.conftest import SuggestionCorpus

PUBLIC_HEALTH = "https://mex.rki.de/item/theme-1"
INFECTIOUS_DISEASES = "https://mex.rki.de/item/theme-11"
BIOINFORMATICS = "https://mex.rki.de/item/theme-23"
ANIMAL_EXPERIMENTAL = "https://mex.rki.de/item/theme-24"
INTERNATIONAL_HEALTH = "https://mex.rki.de/item/theme-37"
OPEN = "https://mex.rki.de/item/access-restriction-1"
RESTRICTED = "https://mex.rki.de/item/access-restriction-2"


def _suggest(client: TestClient, **body: object) -> dict[str, Any]:
    response = client.post("/v0/rule-item/_suggest", json=body)
    assert response.status_code == 200, response.text
    return response.json()  # type: ignore[no-any-return]


def _field(payload: dict[str, Any], name: str) -> dict[str, Any]:
    return next(s for s in payload["suggestions"] if s["field"] == name)


def _supports(payload: dict[str, Any], name: str) -> dict[str, int]:
    return {c["value"]: c["support"] for c in _field(payload, name)["candidates"]}


def _values(payload: dict[str, Any], name: str) -> list[str]:
    return [c["value"] for c in _field(payload, name)["candidates"]]


def test_suggest_needs_an_api_key(client: TestClient) -> None:
    response = client.post(
        "/v0/rule-item/_suggest", json={"additive": {"entityType": "AdditiveResource"}}
    )

    assert response.status_code == 401, response.text


def test_suggest_rejects_an_unknown_entity_type(
    client_with_api_key_read_permission: TestClient,
) -> None:
    response = client_with_api_key_read_permission.post(
        "/v0/rule-item/_suggest", json={"additive": {"entityType": "AdditiveNonsense"}}
    )

    assert response.status_code == 422, response.text


@pytest.mark.parametrize("limit", [0, 99], ids=["too small", "too large"])
def test_suggest_rejects_an_out_of_range_limit(
    client_with_api_key_read_permission: TestClient, limit: int
) -> None:
    response = client_with_api_key_read_permission.post(
        "/v0/rule-item/_suggest",
        json={"additive": {"entityType": "AdditiveResource"}, "limit": limit},
    )

    assert response.status_code == 422, response.text


def test_suggest_is_mounted_on_both_apps(entrypoint_app: TestClient) -> None:
    """Guard against registering the route in only one of the two apps."""
    response = entrypoint_app.post(
        "/v0/rule-item/_suggest", json={"additive": {"entityType": "AdditiveResource"}}
    )

    assert response.status_code != 404, response.text


def test_suggest_with_mocked_graph(
    client_with_api_key_read_permission: TestClient, mocked_graph: MockedGraph
) -> None:
    mocked_graph.return_value = [
        {
            "cohortSize": 4,
            "aggregations": [
                {
                    "field": "theme",
                    "fieldSupport": 4,
                    "candidates": [{"value": PUBLIC_HEALTH, "support": 4}],
                }
            ],
        }
    ]

    payload = _suggest(
        client_with_api_key_read_permission,
        additive={"entityType": "AdditiveResource"},
        fields=["theme"],
    )

    assert payload["entityType"] == "MergedResource"
    assert _values(payload, "theme") == [PUBLIC_HEALTH]
    assert _field(payload, "theme")["source"] == SuggestionSource.CORPUS.value


def test_suggest_without_vocabulary_fields(
    client_with_api_key_read_permission: TestClient, mocked_graph: MockedGraph
) -> None:
    payload = _suggest(
        client_with_api_key_read_permission,
        additive={"entityType": "AdditiveContactPoint"},
    )

    assert payload["suggestions"] == []
    assert mocked_graph.call_args_list == []


@pytest.mark.integration
def test_suggest_recovers_the_cohort_signal(
    client_with_api_key_read_permission: TestClient,
    loaded_suggestion_corpus: SuggestionCorpus,
) -> None:
    payload = _suggest(
        client_with_api_key_read_permission,
        additive={
            "entityType": "AdditiveResource",
            "unitInCharge": [
                str(loaded_suggestion_corpus["unit_alpha"].stableTargetId)
            ],
        },
        fields=["theme", "accessRestriction"],
    )

    assert payload["cohortSize"] == 18
    theme = _field(payload, "theme")
    assert theme["source"] == SuggestionSource.COHORT.value
    # the two resources whose only theme was subtracted leave the denominator
    assert theme["sampleSize"] == 16
    assert _values(payload, "theme")[0] == PUBLIC_HEALTH
    assert _values(payload, "accessRestriction")[0] == OPEN


@pytest.mark.integration
def test_suggest_gives_a_different_answer_per_unit(
    client_with_api_key_read_permission: TestClient,
    loaded_suggestion_corpus: SuggestionCorpus,
) -> None:
    """The same request shape must answer differently for a different unit.

    This is the whole premise of the feature: if both units got the same
    ranking, the conditioning is doing nothing and only the corpus prior is
    speaking.
    """
    payload = _suggest(
        client_with_api_key_read_permission,
        additive={
            "entityType": "AdditiveResource",
            "unitInCharge": [str(loaded_suggestion_corpus["unit_beta"].stableTargetId)],
        },
        fields=["theme", "accessRestriction"],
    )

    assert payload["cohortSize"] == 15
    assert _values(payload, "theme")[0] == BIOINFORMATICS
    assert _supports(payload, "theme")[BIOINFORMATICS] == 13
    assert _values(payload, "accessRestriction")[0] == RESTRICTED


@pytest.mark.integration
def test_suggest_honours_subtractive_rules(
    client_with_api_key_read_permission: TestClient,
    loaded_suggestion_corpus: SuggestionCorpus,
) -> None:
    """Values a curator removed must not come back as evidence.

    Fifteen alpha resources carry `PUBLIC_HEALTH` on their extracted node, but
    three of them have it removed by a subtractive rule, so the support is 12.
    A naive union over all components would report 15 and hand the user back
    exactly the values somebody decided did not belong.
    """
    payload = _suggest(
        client_with_api_key_read_permission,
        additive={
            "entityType": "AdditiveResource",
            "unitInCharge": [
                str(loaded_suggestion_corpus["unit_alpha"].stableTargetId)
            ],
        },
        fields=["theme"],
        limit=10,
    )

    supports = _supports(payload, "theme")
    assert supports[PUBLIC_HEALTH] == 12
    assert supports[INFECTIOUS_DISEASES] == 3
    # the additive rule's replacement value is counted instead
    assert supports[ANIMAL_EXPERIMENTAL] == 1


@pytest.mark.integration
@pytest.mark.usefixtures("loaded_suggestion_corpus")
def test_suggest_recovers_the_text_signal(
    client_with_api_key_read_permission: TestClient,
) -> None:
    for token, expected in [
        ("Kohortenstudie", PUBLIC_HEALTH),
        ("Laborbefund", BIOINFORMATICS),
    ]:
        payload = _suggest(
            client_with_api_key_read_permission,
            additive={
                "entityType": "AdditiveResource",
                "title": [{"value": token, "language": "de"}],
            },
            fields=["theme"],
        )

        assert payload["neighbourhoodSize"] > 0, token
        assert _field(payload, "theme")["source"] == (
            SuggestionSource.NEIGHBOURHOOD.value
        )
        assert _values(payload, "theme")[0] == expected, token


@pytest.mark.integration
@pytest.mark.usefixtures("loaded_suggestion_corpus")
def test_suggest_isolates_entity_types(
    client_with_api_key_read_permission: TestClient,
) -> None:
    """Activities sharing the query token must not vote on a resource's theme."""
    payload = _suggest(
        client_with_api_key_read_permission,
        additive={
            "entityType": "AdditiveResource",
            "title": [{"value": "Kohortenstudie", "language": "de"}],
        },
        fields=["theme"],
    )

    assert INTERNATIONAL_HEALTH not in _values(payload, "theme")
    # 18 alpha resources match the token, but the two whose only theme was
    # subtracted have no effective value left to vote with, so they drop out
    assert payload["neighbourhoodSize"] == 16


@pytest.mark.integration
def test_suggest_falls_back_to_the_corpus_prior(
    client_with_api_key_read_permission: TestClient,
    loaded_suggestion_corpus: SuggestionCorpus,
) -> None:
    """A unit with no resources yields the unconditioned distribution.

    `PUBLIC_HEALTH` holds 14 of the 31 resources that carry any theme, so the
    confidence must be exactly that ratio: with neither signal carrying
    evidence, the blend reduces to the prior itself.
    """
    payload = _suggest(
        client_with_api_key_read_permission,
        additive={
            "entityType": "AdditiveResource",
            "unitInCharge": [
                str(loaded_suggestion_corpus["unit_empty"].stableTargetId)
            ],
        },
        fields=["theme"],
    )

    theme = _field(payload, "theme")
    assert theme["source"] == SuggestionSource.CORPUS.value
    assert theme["sampleSize"] == 31
    assert theme["candidates"][0]["value"] == PUBLIC_HEALTH
    assert theme["candidates"][0]["confidence"] == round(14 / 31, 4)


@pytest.mark.integration
def test_suggest_blends_both_signals(
    client_with_api_key_read_permission: TestClient,
    loaded_suggestion_corpus: SuggestionCorpus,
) -> None:
    """Text pulling the other way must move the confidence, not be ignored."""
    cohort_only = _suggest(
        client_with_api_key_read_permission,
        additive={
            "entityType": "AdditiveResource",
            "unitInCharge": [
                str(loaded_suggestion_corpus["unit_alpha"].stableTargetId)
            ],
        },
        fields=["theme"],
    )
    with_contrary_text = _suggest(
        client_with_api_key_read_permission,
        additive={
            "entityType": "AdditiveResource",
            "unitInCharge": [
                str(loaded_suggestion_corpus["unit_alpha"].stableTargetId)
            ],
            "title": [{"value": "Laborbefund", "language": "de"}],
        },
        fields=["theme"],
    )

    assert _values(cohort_only, "theme")[0] == PUBLIC_HEALTH
    assert (
        _field(with_contrary_text, "theme")["candidates"][0]["confidence"]
        < _field(cohort_only, "theme")["candidates"][0]["confidence"]
    )


@pytest.mark.integration
def test_suggest_excludes_the_item_being_edited(
    client_with_api_key_read_permission: TestClient,
    loaded_suggestion_corpus: SuggestionCorpus,
) -> None:
    majority = next(
        resource
        for resource in loaded_suggestion_corpus["resources"]
        if resource.identifierInPrimarySource == "alpha-majority-0"
    )
    payload = _suggest(
        client_with_api_key_read_permission,
        additive={
            "entityType": "AdditiveResource",
            "unitInCharge": [
                str(loaded_suggestion_corpus["unit_alpha"].stableTargetId)
            ],
        },
        excludeIdentifier=str(majority.stableTargetId),
        fields=["theme"],
    )

    assert payload["cohortSize"] == 17
    assert _supports(payload, "theme")[PUBLIC_HEALTH] == 11


@pytest.mark.integration
@pytest.mark.usefixtures("loaded_suggestion_corpus")
def test_suggest_survives_half_typed_lucene_syntax(
    client_with_api_key_read_permission: TestClient,
) -> None:
    """Unescaped, this query string makes lucene raise and the request 500."""
    payload = _suggest(
        client_with_api_key_read_permission,
        additive={
            "entityType": "AdditiveResource",
            "title": [{"value": "Laborbefund AND (", "language": "de"}],
        },
        fields=["theme"],
    )

    assert _values(payload, "theme")[0] == BIOINFORMATICS
