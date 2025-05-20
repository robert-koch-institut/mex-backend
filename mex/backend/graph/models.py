from collections.abc import Callable, Iterator
from functools import cache
from typing import Any, cast

from neo4j import NotificationSeverity
from neo4j import Result as Neo4jResult
from neo4j._data import RecordExporter
from neo4j.graph import Relationship
from pydantic import BaseModel, field_validator
from typing_extensions import TypedDict

from mex.backend.graph.exceptions import MultipleResultsFoundError, NoResultFoundError
from mex.backend.logging import LOGGING_LINE_LENGTH
from mex.common.logging import logger

GraphValueType = None | str | int | list[str] | list[int]


class EdgeExporter(RecordExporter):
    """Transformer class that turns edges into a string of format `label {props}`.

    Full example:
        `shortName {position: 0}`
    """

    def transform(self, x: Any) -> Any:  # noqa: ANN401
        """Transform a value, or collection of values."""
        if isinstance(x, Relationship):
            properties = ", ".join(f"{k}: {x.get(k)!r}" for k in sorted(x))
            return f"{x.type} {{{properties}}}"
        return super().transform(x)  # type: ignore[no-untyped-call]


class Result:
    """Represent a set of graph results.

    This class wraps `neo4j.Result` in an interface akin to `sqlalchemy.engine.Result`.
    We do this, to reduce vendor tie-in with neo4j and limit the dependency-scope of
    the neo4j driver library to the `mex.backend.graph` submodule.
    """

    def __init__(self, result: Neo4jResult) -> None:
        """Wrap a neo4j result object in a mex-backend result."""
        self._records, self._summary, _ = result.to_eager_result()
        transformer = cast("Callable[[Any], dict[str, Any]]", EdgeExporter().transform)
        self._get_cached_data = cache(
            lambda i: transformer(
                dict(cast("dict[str, Any]", self._records[i]).items())
            )
        )

    def __getitem__(self, key: str) -> Any:  # noqa: ANN401
        """Proxy a getitem instruction to the first record if exactly one exists."""
        return self.one()[key]

    def __iter__(self) -> Iterator[dict[str, Any]]:
        """Return an iterator over all records."""
        yield from (self._get_cached_data(index) for index in range(len(self._records)))

    def __repr__(self) -> str:
        """Return a human-readable representation of this result object."""
        representation = f"Result({self.all()!r})"
        if len(representation) > LOGGING_LINE_LENGTH:
            representation = f"{representation[:40]}... ...{representation[-40:]}"
        return representation

    def log_notifications(self) -> None:
        """Log neo4j notifications."""
        for notification in self._summary.summary_notifications:
            severity = notification.severity_level
            if severity == NotificationSeverity.WARNING:
                logger.warning("%r", notification)
            elif severity == NotificationSeverity.INFORMATION:
                logger.info("%r", notification)
            else:
                logger.debug("%r", notification)

    def all(self) -> list[dict[str, Any]]:
        """Return all records as a list."""
        return list(self)

    def one(self) -> dict[str, Any]:
        """Return exactly one record or raise an exception."""
        match len(self._records):
            case 1:
                return self._get_cached_data(0)
            case 0:
                raise NoResultFoundError from None
            case _:
                raise MultipleResultsFoundError from None

    def one_or_none(self) -> dict[str, Any] | None:
        """Return at most one result or raise an exception.

        Returns None if the result has no records.
        Raises MultipleResultsFound if multiple records are returned.
        """
        match len(self._records):
            case 1:
                return self._get_cached_data(0)
            case 0:
                return None
            case _:
                raise MultipleResultsFoundError from None

    def get_update_counters(self) -> dict[str, int]:
        """Return a summary of counters for operations the query triggered."""
        return {k: v for k, v in vars(self._summary.counters).items() if v}


class GraphRel(TypedDict):
    """Type definition for graph relations."""

    nodeLabels: list[str]
    nodeProps: dict[str, GraphValueType]
    edgeLabel: str
    edgeProps: dict[str, GraphValueType]


class IngestData(BaseModel):
    """Type definition for ingestion data."""

    stableTargetId: str
    identifier: str
    entityType: str
    nodeProps: dict[str, GraphValueType]
    linkRels: list[GraphRel] = []
    createRels: list[GraphRel] = []

    @field_validator("createRels", mode="before")
    @classmethod
    def sort_create_rels(cls, v: list[GraphRel]) -> list[GraphRel]:
        """Sort the rels by edge label and position."""
        return sorted(v, key=lambda x: (x["edgeLabel"], x["edgeProps"]["position"]))

    @field_validator("linkRels", mode="before")
    @classmethod
    def sort_link_rels(cls, v: list[GraphRel]) -> list[GraphRel]:
        """Sort the rels by edge label and position."""
        return sorted(v, key=lambda x: (x["edgeLabel"], x["edgeProps"]["position"]))

    def metadata(self) -> dict[str, int | str]:
        """Return log-able metadata."""
        return {
            "createRels": len(self.createRels),
            "identifier": self.identifier,
            "linkRels": len(self.linkRels),
            "props": len(self.nodeProps),
        }


class IngestParams(BaseModel):
    """Type definition for query parameters."""

    merged_label: str
    node_label: str
    all_referenced_labels: list[str]
    all_nested_labels: list[str]
    detach_node_edges: list[str]
    delete_node_edges: list[str]
    has_link_rels: bool
    has_create_rels: bool
