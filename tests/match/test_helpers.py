import pytest

from mex.backend.exceptions import BackendError
from mex.backend.match.helpers import match_item_in_graph
from mex.backend.rules.helpers import get_rule_set_from_graph
from tests.conftest import DummyData


@pytest.mark.integration
def test_match_unit_1_to_unit_2_creates_rule_set(
    loaded_dummy_data: DummyData,
) -> None:
    unit_1 = loaded_dummy_data["unit_1"]
    unit_2 = loaded_dummy_data["unit_2"]

    assert get_rule_set_from_graph(unit_1.stableTargetId) is None

    with pytest.raises(NotImplementedError):
        match_item_in_graph(unit_1.identifier, unit_2.stableTargetId)

    rule_set = get_rule_set_from_graph(unit_1.stableTargetId)
    assert rule_set is not None
    assert rule_set.stableTargetId == unit_1.stableTargetId


@pytest.mark.integration
def test_match_unit_2_to_unit_1_keeps_existing_rule_set(
    loaded_dummy_data: DummyData,
) -> None:
    unit_1 = loaded_dummy_data["unit_1"]
    unit_2 = loaded_dummy_data["unit_2"]
    unit_2_rule_set = loaded_dummy_data["unit_2_rule_set"]

    rule_set_before = get_rule_set_from_graph(unit_2.stableTargetId)
    assert rule_set_before == unit_2_rule_set

    with pytest.raises(NotImplementedError):
        match_item_in_graph(unit_2.identifier, unit_1.stableTargetId)

    rule_set_after = get_rule_set_from_graph(unit_2.stableTargetId)
    assert rule_set_after == unit_2_rule_set


@pytest.mark.integration
def test_match_item_in_graph_error(
    loaded_dummy_data: DummyData,
) -> None:
    activity_1 = loaded_dummy_data["activity_1"]
    contact_point_2 = loaded_dummy_data["contact_point_2"]

    with pytest.raises(BackendError, match="Failed preconditions"):
        match_item_in_graph(activity_1.identifier, contact_point_2.stableTargetId)
