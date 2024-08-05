from unittest.mock import MagicMock, Mock

import pytest
from neo4j import (
    Record as Neo4jRecord,
)
from neo4j import (
    Result as Neo4jResult,
)
from neo4j import (
    ResultSummary as Neo4jResultSummary,
)

from mex.backend.graph.exceptions import MultipleResultsFoundError, NoResultFoundError
from mex.backend.graph.models import Result
from mex.common.testing import Joker


@pytest.fixture()
def summary() -> Mock:
    class SummaryCounters:
        def __init__(self) -> None:
            self.nodes_created = 73
            self.labels_added = 0
            self.constraints_removed = 0

    return Mock(spec=Neo4jResultSummary, counters=SummaryCounters())


@pytest.fixture()
def multiple_results(summary: Mock) -> Mock:
    records = [
        Mock(spec=Neo4jRecord, data=MagicMock(return_value={"num": 40})),
        Mock(spec=Neo4jRecord, data=MagicMock(return_value={"num": 41})),
        Mock(spec=Neo4jRecord, data=MagicMock(return_value={"num": 42})),
    ]
    return Mock(
        spec=Neo4jResult, to_eager_result=MagicMock(return_value=(records, summary, []))
    )


@pytest.fixture()
def no_result(summary: Mock) -> Mock:
    return Mock(
        spec=Neo4jResult, to_eager_result=MagicMock(return_value=([], summary, []))
    )


@pytest.fixture()
def single_result(summary: Mock) -> Mock:
    records = [
        Mock(
            spec=Neo4jRecord,
            data=MagicMock(
                return_value={
                    "text": "Lorem adipisicing elit consequat sint consectetur "
                    "proident cupidatat culpa voluptate. Aute commodo ea sunt mollit. "
                    "Lorem sint amet reprehenderit aliqua."
                }
            ),
        ),
    ]
    return Mock(
        spec=Neo4jResult, to_eager_result=MagicMock(return_value=(records, summary, []))
    )


def test_result_getitem(multiple_results: Mock, single_result: Mock) -> None:
    # cannot access item when there are multiple results,
    # because that might lead to unexpected results
    with pytest.raises(MultipleResultsFoundError):
        _ = Result(multiple_results)["num"]

    assert Result(single_result)["text"].startswith("Lorem")


def test_result_iter(multiple_results: Mock) -> None:
    assert list(Result(multiple_results)) == [{"num": 40}, {"num": 41}, {"num": 42}]


def test_result_repr(multiple_results: Mock, single_result: Mock) -> None:
    assert (
        repr(Result(multiple_results))
        == "Result([{'num': 40}, {'num': 41}, {'num': 42}])"
    )

    # when representation is too long, it should be abbreviated in the middle
    assert repr(Result(single_result)) == (
        "Result([{'text': 'Lorem adipisicing elit... "
        "...orem sint amet reprehenderit aliqua.'}])"
    )


def test_result_all(multiple_results: Mock) -> None:
    assert Result(multiple_results).all() == [{"num": 40}, {"num": 41}, {"num": 42}]


def test_result_one(
    multiple_results: Mock, no_result: Mock, single_result: Mock
) -> None:
    assert "text" in Result(single_result).one()

    with pytest.raises(NoResultFoundError):
        Result(no_result).one()

    with pytest.raises(MultipleResultsFoundError):
        Result(multiple_results).one()


def test_result_one_or_none(
    multiple_results: Mock, no_result: Mock, single_result: Mock
) -> None:
    assert Result(single_result).one_or_none() == {"text": Joker()}

    assert Result(no_result).one_or_none() is None

    with pytest.raises(MultipleResultsFoundError):
        Result(multiple_results).one_or_none()


def test_get_update_counters(multiple_results: Mock) -> None:
    assert Result(multiple_results).get_update_counters() == {"nodes_created": 73}
