from typing import Callable

from jinja2 import Environment, PackageLoader, select_autoescape

from mex.backend.settings import BackendSettings
from mex.common.models import (
    EXTRACTED_MODEL_CLASSES_BY_NAME,
    MERGED_MODEL_CLASSES_BY_NAME,
)
from mex.common.transform import (
    dromedary_to_kebab,
    dromedary_to_snake,
    ensure_prefix,
    kebab_to_camel,
    snake_to_dromedary,
)
from mex.common.types import Link, Text

NESTED_MODEL_CLASSES_BY_NAME = {cls.__name__: cls for cls in [Link, Text]}


class _QueryBuilder:
    def __init__(self) -> None:
        settings = BackendSettings.get()
        self._env = Environment(
            loader=PackageLoader(__package__, package_path="cypher"),
            autoescape=select_autoescape(),
            auto_reload=settings.debug,
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
        )
        self._env.globals.update(
            extracted_labels=EXTRACTED_MODEL_CLASSES_BY_NAME,
            merged_labels=MERGED_MODEL_CLASSES_BY_NAME,
            nested_labels=NESTED_MODEL_CLASSES_BY_NAME,
        )

    def __getattr__(self, name: str) -> Callable[..., str]:
        template = self._env.get_template(f"{name}.cypher")
        return template.render


q = _QueryBuilder()
