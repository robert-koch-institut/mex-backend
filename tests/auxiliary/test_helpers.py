import pytest
from pytest import MonkeyPatch

from mex.backend.auxiliary import helpers
from mex.backend.auxiliary.constants import (
    LDAP_PRIMARY_SOURCE_NAME,
    RKI_WIKIDATA_ID,
)
from mex.backend.auxiliary.helpers import (
    cached_organization,
    cached_organizational_units,
    cached_primary_source,
)
from mex.backend.extracted.helpers import search_extracted_items_in_graph
from mex.backend.graph.exceptions import NoResultFoundError
from mex.backend.models import ReferenceFilter
from mex.backend.types import ReferenceFieldName
from mex.common.models import (
    ExtractedOrganization,
    ExtractedOrganizationalUnit,
    ExtractedPrimarySource,
)
from mex.common.types import Identifier, Text, TextLanguage
from tests.conftest import get_graph


def test_cached_primary_source() -> None:
    result = cached_primary_source(LDAP_PRIMARY_SOURCE_NAME)
    assert isinstance(result, ExtractedPrimarySource)
    assert result.model_dump() == {
        "hadPrimarySource": "00000000000000",
        "identifierInPrimarySource": "ldap",
        "version": None,
        "alternativeTitle": [],
        "contact": [],
        "contributor": [],
        "description": [],
        "documentation": [],
        "locatedAt": [],
        "title": [{"value": "Active Directory", "language": TextLanguage.EN}],
        "unitInCharge": [],
        "entityType": "ExtractedPrimarySource",
        "identifier": "cmiaN880A6fm1Ggno4kl7m",
        "stableTargetId": "ebs5siX85RkdrhBRlsYgRP",
    }
    # the result is cached and the same object is returned on subsequent calls
    assert cached_primary_source(LDAP_PRIMARY_SOURCE_NAME) is result


def test_cached_primary_source_not_found() -> None:
    with pytest.raises(NoResultFoundError):
        cached_primary_source("this-primary-source-does-not-exist")


@pytest.mark.integration
def test_cached_primary_source_ingest() -> None:
    # verify the primary source has been stored in the database
    result = cached_primary_source(LDAP_PRIMARY_SOURCE_NAME)

    ingested = search_extracted_items_in_graph(
        reference_filters=[
            ReferenceFilter(
                field=ReferenceFieldName("stableTargetId"),
                identifiers=[Identifier(result.stableTargetId)],
            )
        ],
        entity_type=["ExtractedPrimarySource"],
    )

    assert ingested.total == 1, get_graph()


@pytest.mark.usefixtures("mocked_wikidata")
def test_cached_organization() -> None:
    result = cached_organization(RKI_WIKIDATA_ID)
    assert isinstance(result, ExtractedOrganization)
    assert result.identifierInPrimarySource == RKI_WIKIDATA_ID
    assert result.wikidataId == ["http://www.wikidata.org/entity/Q679041"]
    # the result is cached and the same object is returned on subsequent calls
    assert cached_organization(RKI_WIKIDATA_ID) is result


@pytest.mark.usefixtures("mocked_wikidata")
def test_cached_organization_not_found(monkeypatch: MonkeyPatch) -> None:
    monkeypatch.setattr(
        helpers,
        "transform_wikidata_organization_to_extracted_organization",
        lambda *_, **__: None,
    )
    with pytest.raises(NoResultFoundError):
        cached_organization(RKI_WIKIDATA_ID)


@pytest.mark.usefixtures("mocked_wikidata")
def test_cached_organizational_units() -> None:
    expected_result = [
        ExtractedOrganizationalUnit(
            hadPrimarySource="dsnYIq1AxYMLcTbSIBvDSs",
            identifierInPrimarySource="child-unit",
            name=[
                Text(value="CHLD Unterabteilung", language=TextLanguage.DE),
                Text(value="C1: Sub Unit", language=TextLanguage.EN),
            ],
            alternativeName=[
                Text(value="CHLD", language=TextLanguage.EN),
                Text(value="C1 Sub-Unit", language=TextLanguage.EN),
                Text(value="C1 Unterabteilung", language=TextLanguage.DE),
            ],
            shortName=[Text(value="C1", language=TextLanguage.DE)],
            unitOf=["ga6xh6pgMwgq7DC7r6Wjqg"],
            identifier="dHpMfrmbV1PQBkaShNv7kp",
            stableTargetId="6rqNvZSApUHlz8GkkVP48",
        ),
        ExtractedOrganizationalUnit(
            hadPrimarySource="dsnYIq1AxYMLcTbSIBvDSs",
            identifierInPrimarySource="parent-unit",
            name=[
                Text(value="Abteilung", language=TextLanguage.DE),
                Text(value="Department", language=TextLanguage.EN),
            ],
            alternativeName=[
                Text(value="PRNT Abteilung", language=TextLanguage.DE),
                Text(value="PARENT Dept.", language=None),
            ],
            email=["pu@example.com", "PARENT@example.com"],
            shortName=[Text(value="PRNT", language=TextLanguage.DE)],
            unitOf=["ga6xh6pgMwgq7DC7r6Wjqg"],
            identifier="hB7EDcR0F24d0JbfwvJ2ub",
            stableTargetId="hIiJpZXVppHvoyeP0QtAoS",
        ),
        ExtractedOrganizationalUnit(
            hadPrimarySource="dsnYIq1AxYMLcTbSIBvDSs",
            identifierInPrimarySource="fg99",
            name=[
                Text(value="Fachgebiet 99", language=TextLanguage.DE),
                Text(value="Group 99", language=TextLanguage.EN),
            ],
            alternativeName=[Text(value="FG99", language=TextLanguage.DE)],
            email=["fg@example.com"],
            shortName=[Text(value="FG 99", language=TextLanguage.DE)],
            unitOf=["ga6xh6pgMwgq7DC7r6Wjqg"],
            identifier="hCwNEsnCvG9kFf9qDVHxSM",
            stableTargetId="cjna2jitPngp6yIV63cdi9",
        ),
    ]
    result = cached_organizational_units()
    assert result == expected_result

    # check org units are ingested into graph
    for expected_unit in expected_result:
        ingested = search_extracted_items_in_graph(
            reference_filters=[
                ReferenceFilter(
                    field=ReferenceFieldName("stableTargetId"),
                    identifiers=[expected_unit.stableTargetId],
                )
            ],
        )
        assert ingested.total == 1, get_graph()


def test_cached_organizational_units_empty(monkeypatch: MonkeyPatch) -> None:
    monkeypatch.setattr(helpers, "extract_organigram_units", list)
    with pytest.raises(NoResultFoundError):
        cached_organizational_units()
