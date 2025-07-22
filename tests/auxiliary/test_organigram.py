import pytest

from mex.backend.auxiliary.organigram import extracted_organizational_units
from mex.backend.extracted.helpers import search_extracted_items_in_graph
from mex.common.models import ExtractedOrganizationalUnit
from mex.common.types import Text, TextLanguage
from tests.conftest import get_graph


@pytest.mark.usefixtures("mocked_wikidata")
def test_extracted_organizational_unit() -> None:
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
    result = extracted_organizational_units()
    assert result == expected_result

    # check org units are ingested into graph
    ingested_org_unit_1 = search_extracted_items_in_graph(
        stable_target_id=expected_result[0].stableTargetId,
    )
    assert ingested_org_unit_1.total == 1, get_graph()
    ingested_org_unit_2 = search_extracted_items_in_graph(
        stable_target_id=expected_result[1].stableTargetId,
    )
    assert ingested_org_unit_2.total == 1, get_graph()
    ingested_org_unit_3 = search_extracted_items_in_graph(
        stable_target_id=expected_result[2].stableTargetId,
    )
    assert ingested_org_unit_3.total == 1, get_graph()
