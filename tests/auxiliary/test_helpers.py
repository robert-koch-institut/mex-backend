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
    ExtractedPrimarySource,
)
from mex.common.types import Identifier
from tests.conftest import get_graph


def test_cached_primary_source() -> None:
    result = cached_primary_source(LDAP_PRIMARY_SOURCE_NAME)
    assert isinstance(result, ExtractedPrimarySource)
    assert result.identifierInPrimarySource == "ldap"
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
    result = cached_organizational_units()
    assert len(result) == 3
    assert result[0].identifierInPrimarySource == "child-unit"

    # check org units are ingested into graph
    ingested = search_extracted_items_in_graph(
        reference_filters=[
            ReferenceFilter(
                field=ReferenceFieldName("stableTargetId"),
                identifiers=[r.stableTargetId for r in result],
            )
        ],
    )
    assert ingested.total == 3, get_graph()


@pytest.mark.usefixtures("mocked_wikidata")
def test_cached_organizational_units_empty(monkeypatch: MonkeyPatch) -> None:
    monkeypatch.setattr(helpers, "extract_organigram_units", list)
    with pytest.raises(NoResultFoundError):
        cached_organizational_units()
