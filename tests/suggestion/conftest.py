from collections import deque
from typing import TypedDict

import pytest

from mex.backend.graph.connector import GraphConnector
from mex.common.models import (
    MEX_PRIMARY_SOURCE_STABLE_TARGET_ID,
    AdditiveResource,
    AnyExtractedModel,
    AnyRuleSetResponse,
    ExtractedActivity,
    ExtractedContactPoint,
    ExtractedOrganizationalUnit,
    ExtractedPrimarySource,
    ExtractedResource,
    ResourceRuleSetResponse,
    SubtractiveResource,
)
from mex.common.types import AccessRestriction, Text, TextLanguage, Theme


class SuggestionCorpus(TypedDict):
    """A corpus with a planted, hand-computable vocabulary distribution."""

    primary_source: ExtractedPrimarySource
    contact_point: ExtractedContactPoint
    unit_alpha: ExtractedOrganizationalUnit
    unit_beta: ExtractedOrganizationalUnit
    unit_empty: ExtractedOrganizationalUnit
    resources: list[ExtractedResource]
    rule_sets: list[ResourceRuleSetResponse]
    activities: list[ExtractedActivity]


@pytest.fixture
def suggestion_corpus() -> SuggestionCorpus:
    """Build a corpus whose conditional vocabulary distribution is known exactly.

    Neither existing corpus can show that the suggestion mechanism recovers a
    signal: `dummy_data` holds a single vocabulary value and `artificial_data`
    draws them at random, so by construction it contains no conditional
    structure at all. This one plants a structure that the assertions can then
    demand back, with counts chosen so that nothing ties.

    Effective theme distribution, after additive and subtractive rules:

    | cohort | items | theme                              | count |
    |--------|-------|------------------------------------|-------|
    | alpha  | 18    | PUBLIC_HEALTH                      | 12    |
    |        |       | INFECTIOUS_DISEASES_AND_EPIDEMIOLOGY | 3   |
    |        |       | ANIMAL_EXPERIMENTAL_RESEARCH_AND_3R | 1    |
    |        |       | (no effective value)               | 2     |
    | beta   | 15    | BIOINFORMATICS_AND_SYSTEMS_BIOLOGY | 13    |
    |        |       | PUBLIC_HEALTH                      | 2     |

    Three alpha resources carry `PUBLIC_HEALTH` on their extracted node and have
    it removed by a subtractive rule, so a naive component union would report 15
    rather than 12. That gap is what the subtractive assertions pin.
    """
    primary_source = ExtractedPrimarySource(
        hadPrimarySource=MEX_PRIMARY_SOURCE_STABLE_TARGET_ID,
        identifierInPrimarySource="ps-suggestion",
    )
    contact_point = ExtractedContactPoint(
        email=["suggest@contact-point.one"],
        hadPrimarySource=primary_source.stableTargetId,
        identifierInPrimarySource="cp-suggestion",
    )
    unit_alpha = ExtractedOrganizationalUnit(
        hadPrimarySource=primary_source.stableTargetId,
        identifierInPrimarySource="ou-alpha",
        name=[Text(value="Abteilung Alpha", language=TextLanguage.DE)],
    )
    unit_beta = ExtractedOrganizationalUnit(
        hadPrimarySource=primary_source.stableTargetId,
        identifierInPrimarySource="ou-beta",
        name=[Text(value="Abteilung Beta", language=TextLanguage.DE)],
    )
    unit_empty = ExtractedOrganizationalUnit(
        hadPrimarySource=primary_source.stableTargetId,
        identifierInPrimarySource="ou-empty",
        name=[Text(value="Abteilung Ohne Ressourcen", language=TextLanguage.DE)],
    )

    def make_resource(
        slug: str,
        unit: ExtractedOrganizationalUnit,
        theme: Theme,
        access_restriction: AccessRestriction,
        title: str,
    ) -> ExtractedResource:
        return ExtractedResource(
            accessRestriction=access_restriction,
            contact=[contact_point.stableTargetId],
            hadPrimarySource=primary_source.stableTargetId,
            identifierInPrimarySource=slug,
            theme=[theme],
            title=[Text(value=title, language=TextLanguage.DE)],
            unitInCharge=[unit.stableTargetId],
        )

    resources: list[ExtractedResource] = []
    rule_sets: list[ResourceRuleSetResponse] = []

    # alpha: 12 public health, 3 infectious diseases, all openly accessible
    resources.extend(
        make_resource(
            f"alpha-majority-{index}",
            unit_alpha,
            Theme["PUBLIC_HEALTH"],
            AccessRestriction["OPEN"],
            f"Kohortenstudie Alpha {index}",
        )
        for index in range(12)
    )
    resources.extend(
        make_resource(
            f"alpha-minority-{index}",
            unit_alpha,
            Theme["INFECTIOUS_DISEASES_AND_EPIDEMIOLOGY"],
            AccessRestriction["OPEN"],
            f"Kohortenstudie Alpha Minderheit {index}",
        )
        for index in range(3)
    )
    # alpha: two resources whose only theme is removed again by a rule, so they
    # drop out of the theme denominator without dropping out of the cohort
    subtracted_resources = [
        make_resource(
            f"alpha-subtracted-{index}",
            unit_alpha,
            Theme["PUBLIC_HEALTH"],
            AccessRestriction["OPEN"],
            f"Kohortenstudie Alpha Subtrahiert {index}",
        )
        for index in range(2)
    ]
    resources.extend(subtracted_resources)
    rule_sets.extend(
        ResourceRuleSetResponse(
            subtractive=SubtractiveResource(theme=[Theme["PUBLIC_HEALTH"]]),
            stableTargetId=resource.stableTargetId,
        )
        for resource in subtracted_resources
    )
    # alpha: one resource whose theme is swapped out entirely by a rule
    ruled_resource = make_resource(
        "alpha-ruled-0",
        unit_alpha,
        Theme["PUBLIC_HEALTH"],
        AccessRestriction["OPEN"],
        "Kohortenstudie Alpha Reguliert",
    )
    resources.append(ruled_resource)
    rule_sets.append(
        ResourceRuleSetResponse(
            additive=AdditiveResource(
                theme=[Theme["ANIMAL_EXPERIMENTAL_RESEARCH_AND_3R"]]
            ),
            subtractive=SubtractiveResource(theme=[Theme["PUBLIC_HEALTH"]]),
            stableTargetId=ruled_resource.stableTargetId,
        )
    )

    # beta: 13 bioinformatics, 2 public health, mostly restricted
    resources.extend(
        make_resource(
            f"beta-majority-{index}",
            unit_beta,
            Theme["BIOINFORMATICS_AND_SYSTEMS_BIOLOGY"],
            AccessRestriction["OPEN" if index < 3 else "RESTRICTED"],
            f"Laborbefund Beta {index}",
        )
        for index in range(13)
    )
    resources.extend(
        make_resource(
            f"beta-minority-{index}",
            unit_beta,
            Theme["PUBLIC_HEALTH"],
            AccessRestriction["RESTRICTED"],
            f"Laborbefund Beta Minderheit {index}",
        )
        for index in range(2)
    )

    # activities sharing the alpha text token, to pin entity type isolation
    activities = [
        ExtractedActivity(
            contact=[contact_point.stableTargetId],
            hadPrimarySource=primary_source.stableTargetId,
            identifierInPrimarySource=f"activity-{index}",
            responsibleUnit=[unit_alpha.stableTargetId],
            theme=[Theme["INTERNATIONAL_HEALTH_PROTECTION"]],
            title=[
                Text(
                    value=f"Kohortenstudie Aktivität {index}", language=TextLanguage.DE
                )
            ],
        )
        for index in range(3)
    ]

    return SuggestionCorpus(
        primary_source=primary_source,
        contact_point=contact_point,
        unit_alpha=unit_alpha,
        unit_beta=unit_beta,
        unit_empty=unit_empty,
        resources=resources,
        rule_sets=rule_sets,
        activities=activities,
    )


@pytest.fixture
def loaded_suggestion_corpus(suggestion_corpus: SuggestionCorpus) -> SuggestionCorpus:
    """Ingest the planted corpus into the graph."""
    items: list[AnyExtractedModel | AnyRuleSetResponse] = [
        suggestion_corpus["primary_source"],
        suggestion_corpus["contact_point"],
        suggestion_corpus["unit_alpha"],
        suggestion_corpus["unit_beta"],
        suggestion_corpus["unit_empty"],
        *suggestion_corpus["resources"],
        *suggestion_corpus["activities"],
        *suggestion_corpus["rule_sets"],
    ]
    connector = GraphConnector.get()
    deque(connector.ingest_items(items))
    return suggestion_corpus
