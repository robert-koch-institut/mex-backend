from typing import Any, Iterator

from neo4j import Result as Neo4jResult
from pydantic import BaseModel, Field

from mex.backend.graph.exceptions import MultipleResultsFound, NoResultFound


class Result:
    """Represent a set of graph results.

    This class wraps `neo4j.Result` in an interface akin to `sqlalchemy.engine.Result`.
    We do this, to reduce vendor tie-in with neo4j and limit the dependency-scope of
    the neo4j driver library to the `mex.backend.graph` submodule.
    """

    def __init__(self, result: Neo4jResult) -> None:
        """Wrap a neo4j result object in a mex-backend result."""
        self._records, self._summary, _ = result.to_eager_result()

    def __getitem__(self, key: str) -> Any:
        """Proxy a getitem instruction to the first record if exactly one exists."""
        return self.one()[key]

    def __iter__(self) -> Iterator[dict[str, Any]]:
        """Return an iterator over all records."""
        yield from (record.data() for record in self._records)

    def __repr__(self) -> str:
        """Return a human-readable representation of this result object."""
        representation = f"Result({self.all()!r})"
        if len(representation) > 120:
            representation = f"{representation[:60]}...{representation[57:]}"
        return representation

    def all(self) -> list[dict[str, Any]]:
        """Return all records as a list."""
        return list(self)

    def one(self) -> dict[str, Any]:
        """Return exactly one record or raise an exception."""
        match len(self._records):
            case 1:
                return self._records[0].data()
            case 0:
                raise NoResultFound
            case _:
                raise MultipleResultsFound

    def one_or_none(self) -> dict[str, Any] | None:
        """Return at most one result or raise an exception.

        Returns None if the result has no records.
        Raises MultipleResultsFound if multiple records are returned.
        """
        match len(self._records):
            case 1:
                return self._records[0].data()
            case 0:
                return None
            case _:
                raise MultipleResultsFound

    @property
    def update_counters(self) -> dict[str, int]:
        """Return a summary of counters for operations the query triggered."""
        return {k: v for k, v in vars(self._summary.counters).items() if v}


class MergableEdge(BaseModel):
    """Helper class for merging edges into the graph."""

    label: str = Field(exclude=False)
    fromIdentifier: str
    toStableTargetId: str
    position: int
