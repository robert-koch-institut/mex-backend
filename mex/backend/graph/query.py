from collections.abc import Callable
from functools import cache
from typing import Annotated, Any

from black import Mode, format_str
from jinja2 import (
    Environment,
    PackageLoader,
    StrictUndefined,
    Template,
    select_autoescape,
)
from pydantic import StringConstraints, validate_call

from mex.backend.fields import (
    NESTED_ENTITY_TYPES_BY_CLASS_NAME,
    REFERENCED_ENTITY_TYPES_BY_CLASS_NAME,
)
from mex.backend.graph.models import IngestParams
from mex.backend.settings import BackendSettings
from mex.common.connector import BaseConnector
from mex.common.fields import (
    LINK_FIELDS_BY_CLASS_NAME,
    REFERENCE_FIELDS_BY_CLASS_NAME,
    TEXT_FIELDS_BY_CLASS_NAME,
)
from mex.common.models import (
    EXTRACTED_MODEL_CLASSES_BY_NAME,
    MERGED_MODEL_CLASSES_BY_NAME,
    RULE_MODEL_CLASSES_BY_NAME,
)
from mex.common.transform import (
    dromedary_to_kebab,
    dromedary_to_snake,
    ensure_postfix,
    ensure_prefix,
    kebab_to_camel,
    snake_to_dromedary,
)
from mex.common.types import NESTED_MODEL_CLASSES_BY_NAME

# TODO(ND): move this to mex-common
EXTRACTED_AND_RULE_MODEL_CLASSES_BY_NAME = {
    **EXTRACTED_MODEL_CLASSES_BY_NAME,
    **RULE_MODEL_CLASSES_BY_NAME,
}


@validate_call
def render_constraints(
    fields: list[Annotated[str, StringConstraints(pattern=r"^[a-zA-Z]{1,255}$")]],
) -> str:
    """Convert a list of field names into cypher node/edge constraints."""
    return ", ".join(f"{f}: ${f}" for f in fields)


class Query:
    """Wrapper for queries that can be rendered."""

    REPR_MODE = Mode(line_length=1024)

    def __init__(self, name: str, template: Template, kwargs: dict[str, Any]) -> None:
        """Create a new query instance."""
        self.name = name
        self.template = template
        self.kwargs = kwargs

    def __str__(self) -> str:
        """Render the query for database execution."""
        return self.template.render(**self.kwargs)

    def __repr__(self) -> str:
        """Render the call to the query builder for logging and testing."""
        kwargs_repr = ",".join(f"{k}={v!r}" for k, v in self.kwargs.items())
        return format_str(f"{self.name}({kwargs_repr})", mode=self.REPR_MODE).strip()


class QueryBuilder(BaseConnector):
    """Wrapper around jinja template loading and rendering."""

    def __init__(self) -> None:
        """Create a new jinja environment with template loader, filters and globals."""
        settings = BackendSettings.get()
        self._env = Environment(
            loader=PackageLoader(__package__, package_path="cypher"),
            autoescape=select_autoescape(),
            auto_reload=settings.debug,
            undefined=StrictUndefined,
            block_start_string="<%",
            block_end_string="%>",
            variable_start_string="<<",
            variable_end_string=">>",
            comment_start_string="<#",
            comment_end_string="#>",
        )
        self._env.filters.update(
            snake_to_dromedary=snake_to_dromedary,
            dromedary_to_snake=dromedary_to_snake,
            dromedary_to_kebab=dromedary_to_kebab,
            kebab_to_camel=kebab_to_camel,
            ensure_prefix=ensure_prefix,
            ensure_postfix=ensure_postfix,
            render_constraints=render_constraints,
        )
        self._env.globals.update(
            extracted_labels=list(EXTRACTED_MODEL_CLASSES_BY_NAME),
            merged_labels=list(MERGED_MODEL_CLASSES_BY_NAME),
            nested_labels=list(NESTED_MODEL_CLASSES_BY_NAME),
            rule_labels=list(RULE_MODEL_CLASSES_BY_NAME),
            extracted_or_rule_labels=list(EXTRACTED_MODEL_CLASSES_BY_NAME)
            + list(RULE_MODEL_CLASSES_BY_NAME),
        )
        self.get_ingest_query_for_entity_type = cache(
            self._get_ingest_query_for_entity_type
        )

    def __getattr__(self, name: str) -> Callable[..., Query]:
        """Load the template with the given `name` and return a query factory."""
        template = self._env.get_template(f"{name}.cql")
        return lambda **kwargs: Query(name, template, kwargs)

    def _get_ingest_query_for_entity_type(self, entity_type: str) -> str:
        """Create an ingest query for the given entity type.

        Generates a complex Cypher query template for ingesting extracted or rule
        models into the graph database. The query handles creation of nodes, nested
        objects (Text, Link), and reference relationships. Results are cached
        for performance.

        Args:
            entity_type: The entity type name (e.g. "ExtractedPerson")

        Raises:
            KeyError: If the entity type is not found in the model classes

        Returns:
            Cypher query string template for ingesting this entity type
        """
        stem_type = EXTRACTED_AND_RULE_MODEL_CLASSES_BY_NAME[entity_type].stemType
        merged_label = ensure_prefix(stem_type, "Merged")
        text_fields = TEXT_FIELDS_BY_CLASS_NAME[entity_type]
        link_fields = LINK_FIELDS_BY_CLASS_NAME[entity_type]
        nested_types_for_class = NESTED_ENTITY_TYPES_BY_CLASS_NAME[entity_type]
        ref_fields_for_class = REFERENCE_FIELDS_BY_CLASS_NAME[entity_type]
        ref_fields = sorted(set(ref_fields_for_class) - {"stableTargetId"})
        ref_types_for_class = REFERENCED_ENTITY_TYPES_BY_CLASS_NAME[entity_type]
        params = IngestParams(
            merged_label=merged_label,
            node_label=entity_type,
            all_referenced_labels=ref_types_for_class,
            all_nested_labels=nested_types_for_class,
            detach_node_edges=ref_fields,
            delete_node_edges=[*text_fields, *link_fields],
            has_link_rels=bool(ref_types_for_class),
            has_create_rels=bool(nested_types_for_class),
        )
        query_builder = QueryBuilder.get()
        query = query_builder.merge_item(params=params)
        return str(query)

    def close(self) -> None:
        """Clean up the connector."""
        self.get_ingest_query_for_entity_type.cache_clear()
