from itertools import groupby
from typing import TypedDict

from mex.common.types import AnyPrimitiveType


class _SearchResultReference(TypedDict):
    """Helper class to show the structure of search result references."""

    label: str  # corresponds to the field name of the pydantic model
    position: int  # if the field in pydantic is a list this helps keep its order
    value: str | dict[str, str | None]  # this can be a raw Identifier, Text or Link


def expand_references_in_search_result(
    refs: list[_SearchResultReference],
) -> dict[str, list[str | dict[str, str | None]]]:
    """Expand the `_refs` collection in a search result item.

    Each item in a search result has a collection of `_refs` in the form of
    `_SearchResultReference`. Before parsing them into pydantic, we need to inline
    the references back into the `item` dictionary.
    """
    # TODO(ND): try to re-write directly in the cypher query, if we can use `apoc`
    sorted_refs = sorted(refs, key=lambda ref: (ref["label"], ref["position"]))
    groups = groupby(sorted_refs, lambda ref: (ref["label"]))
    return {label: [ref["value"] for ref in group] for label, group in groups}


def transform_edges_into_expectations_by_edge_locator(
    start_node_type: str,
    start_node_constraints: dict[str, AnyPrimitiveType],
    ref_labels: list[str],
    ref_identifiers: list[str],
    ref_positions: list[int],
) -> dict[str, str]:
    """Generate a all expected edges and render a CYPHER-style merge statement."""
    start_node = ", ".join(f'{k}: "{v!s}"' for k, v in start_node_constraints.items())
    return {
        (edge_locator := f"{label} {{position: {position}}}"): (
            f"(:{start_node_type} {{{start_node}}})"
            f"-[:{edge_locator}]->"
            f'({{identifier: "{identifier}"}})'
        )
        for label, position, identifier in zip(
            ref_labels, ref_positions, ref_identifiers, strict=True
        )
    }
