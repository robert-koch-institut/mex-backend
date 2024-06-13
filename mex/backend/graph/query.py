from collections.abc import Callable
from typing import Annotated

from jinja2 import Environment, PackageLoader, StrictUndefined, select_autoescape
from pydantic import StringConstraints, validate_call

from mex.backend.settings import BackendSettings
from mex.common.connector import BaseConnector
from mex.common.models import (
    EXTRACTED_MODEL_CLASSES_BY_NAME,
    MERGED_MODEL_CLASSES_BY_NAME,
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


@validate_call
def render_constraints(
    fields: list[Annotated[str, StringConstraints(pattern=r"^[a-zA-Z]{1,255}$")]]
) -> str:
    """Convert a list of field names into cypher node/edge constraints."""
    return ", ".join(f"{f}: ${f}" for f in fields)


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
        )

    def __getattr__(self, name: str) -> Callable[..., str]:
        """Load the template with the given `name` and return its `render` method."""
        template = self._env.get_template(f"{name}.cql")
        return template.render

    def close(self) -> None:
        """Clean up the connector."""
        pass  # no clean-up needed
