from mex.backend.rules.helpers import build_tombstone_rule_set
from mex.common.models import (
    MergedContactPoint,
    MergedOrganizationalUnit,
)
from mex.common.types import (
    MergedContactPointIdentifier,
    MergedOrganizationalUnitIdentifier,
)

GONER = MergedOrganizationalUnitIdentifier("bFQoRhcVH5DHUA")
KEEPER = MergedOrganizationalUnitIdentifier("bFQoRhcVH5DHUB")


def test_build_tombstone_rule_set() -> None:
    goner = MergedOrganizationalUnit(identifier=GONER, name=[{"value": "Unit 1"}])
    keeper = MergedOrganizationalUnit(identifier=KEEPER, name=[{"value": "Unit 2"}])

    rule_set = build_tombstone_rule_set(goner, keeper)

    assert rule_set.entityType == "OrganizationalUnitRuleSetResponse"
    assert rule_set.stableTargetId == GONER
    assert rule_set.additive.supersededBy == KEEPER
    # nothing but supersededBy survives, that is what makes it a tombstone
    assert rule_set.additive.model_dump(exclude_defaults=True) == {
        "supersededBy": KEEPER
    }
    assert rule_set.subtractive.model_dump(exclude_defaults=True) == {}
    assert rule_set.preventive.model_dump(exclude_defaults=True) == {}
    assert rule_set.workflow.model_dump(exclude_defaults=True) == {}


def test_build_tombstone_rule_set_picks_the_matching_stem_type() -> None:
    goner = MergedContactPoint(
        identifier=MergedContactPointIdentifier("bFQoRhcVH5DHUC"),
        email=["one@contact-point.tld"],
    )
    keeper = MergedContactPoint(
        identifier=MergedContactPointIdentifier("bFQoRhcVH5DHUD"),
        email=["two@contact-point.tld"],
    )

    rule_set = build_tombstone_rule_set(goner, keeper)

    assert rule_set.entityType == "ContactPointRuleSetResponse"
    assert rule_set.additive.entityType == "AdditiveContactPoint"
    assert rule_set.additive.supersededBy == keeper.identifier
