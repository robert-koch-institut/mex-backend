from itertools import groupby
from typing import Any, TypedDict

from pydantic_core import ErrorDetails

from mex.backend.graph.models import GraphRel, IngestData
from mex.common.fields import (
    FINAL_FIELDS_BY_CLASS_NAME,
    LINK_FIELDS_BY_CLASS_NAME,
    MUTABLE_FIELDS_BY_CLASS_NAME,
    REFERENCE_FIELDS_BY_CLASS_NAME,
    TEXT_FIELDS_BY_CLASS_NAME,
)
from mex.common.models import AnyExtractedModel
from mex.common.transform import ensure_prefix, to_key_and_values
from mex.common.types import AnyPrimitiveType, Link, Text


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


def transform_model_into_ingest_data(model: AnyExtractedModel) -> IngestData:
    """Transform the given model into an ingestion instruction."""
    merged_label = ensure_prefix(model.stemType, "Merged")

    text_fields = TEXT_FIELDS_BY_CLASS_NAME[model.entityType]
    link_fields = LINK_FIELDS_BY_CLASS_NAME[model.entityType]
    mutable_fields = MUTABLE_FIELDS_BY_CLASS_NAME[model.entityType]
    final_fields = FINAL_FIELDS_BY_CLASS_NAME[model.entityType]
    ref_fields = sorted(
        set(REFERENCE_FIELDS_BY_CLASS_NAME[model.entityType]) - {"stableTargetId"}
    )

    mutable_values = model.model_dump(include=set(mutable_fields))
    final_values = model.model_dump(include=set(final_fields))
    all_values = {**mutable_values, **final_values}

    text_values = model.model_dump(include=set(text_fields))
    link_values = model.model_dump(include=set(link_fields))

    ref_values = model.model_dump(include=set(ref_fields))

    nested_texts = []
    for edge_label, raw_values in to_key_and_values(text_values):
        for position, raw_value in enumerate(raw_values):
            nested_texts.append(
                GraphRel(
                    edgeLabel=edge_label,
                    edgeProps={"position": position},
                    nodeLabels=[Text.__name__],
                    nodeProps=raw_value,
                )
            )

    nested_links = []
    for edge_label, raw_values in to_key_and_values(link_values):
        for position, raw_value in enumerate(raw_values):
            nested_links.append(
                GraphRel(
                    edgeLabel=edge_label,
                    edgeProps={"position": position},
                    nodeLabels=[Link.__name__],
                    nodeProps=raw_value,
                )
            )

    link_rels = []
    for field, identifiers in to_key_and_values(ref_values):
        for position, identifier in enumerate(identifiers):
            link_rels.append(
                GraphRel(
                    edgeLabel=field,
                    edgeProps={"position": position},
                    nodeLabels=[],  # list(MERGED_MODEL_CLASSES_BY_NAME),
                    nodeProps={"identifier": identifier},
                )
            )

    return IngestData(
        identifier=str(model.identifier),
        stableTargetId=str(model.stableTargetId),
        mergedLabels=[merged_label],
        nodeLabels=[model.entityType],
        nodeProps=all_values,
        linkRels=link_rels,
        nestedTexts=nested_texts,
        nestedLinks=nested_links,
        detachNodes=ref_fields,
        deleteNodes=[*text_fields, *link_fields],
    )


def clean_dict(obj: Any) -> Any:
    """Clean `None` and `[]` from lists."""
    if isinstance(obj, dict):
        cleaned = {}
        for k, v in obj.items():
            cleaned_value = clean_dict(v)
            if cleaned_value is not None and cleaned_value != []:
                cleaned[k] = cleaned_value
        return cleaned
    if isinstance(obj, list):
        return [clean_dict(item) for item in obj]
    return obj


def validate_ingested_data(
    data_in: IngestData, data_out: IngestData
) -> list[ErrorDetails]:
    """Validate that the ingestion has been executed successfully."""
    error_details = []
    for field in IngestData.model_fields:
        value_in = clean_dict(getattr(data_in, field))
        value_out = clean_dict(getattr(data_out, field))
        if value_out != value_in:
            error_details.append(
                ErrorDetails(
                    type="mismatch",
                    msg="ingested data did not match instructions",
                    loc=(field,),
                    input=value_out,
                    ctx={"expected": value_in},
                )
            )
    return error_details
