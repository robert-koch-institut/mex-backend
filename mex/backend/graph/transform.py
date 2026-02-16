from itertools import groupby
from typing import TYPE_CHECKING, Any, TypedDict, cast

from pydantic_core import ErrorDetails

from mex.backend.fields import REFERENCED_ENTITY_TYPES_BY_FIELD_BY_CLASS_NAME
from mex.backend.graph.models import GraphRel, IngestData, MExPrimarySource
from mex.common.fields import (
    FINAL_FIELDS_BY_CLASS_NAME,
    LINK_FIELDS_BY_CLASS_NAME,
    MUTABLE_FIELDS_BY_CLASS_NAME,
    REFERENCE_FIELDS_BY_CLASS_NAME,
    TEXT_FIELDS_BY_CLASS_NAME,
)
from mex.common.models import AnyExtractedModel, AnyRuleModel, AnyRuleSetResponse
from mex.common.transform import to_key_and_values
from mex.common.types import AnyPrimitiveType, Link, Text

if TYPE_CHECKING:
    from neo4j.exceptions import Neo4jError


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


def transform_model_into_ingest_data(
    model: AnyExtractedModel | MExPrimarySource | AnyRuleModel,
    stable_target_id: str,
) -> IngestData:
    """Transform the given model into an ingestion instruction.

    Converts an extracted or rule model into structured data ready for database
    ingestion. Handles field categorization (mutable vs final), reference
    field processing, and nested object preparation.

    Args:
        model: The extracted or rule model to transform for ingestion
        stable_target_id: Identifier of the associated merged item

    Returns:
        IngestData object containing query parameters and metadata
    """
    text_fields = TEXT_FIELDS_BY_CLASS_NAME[model.entityType]
    link_fields = LINK_FIELDS_BY_CLASS_NAME[model.entityType]
    mutable_fields = MUTABLE_FIELDS_BY_CLASS_NAME[model.entityType]
    final_fields = FINAL_FIELDS_BY_CLASS_NAME[model.entityType]
    ref_fields_for_class = REFERENCE_FIELDS_BY_CLASS_NAME[model.entityType]
    ref_fields = sorted(set(ref_fields_for_class) - {"stableTargetId"})
    ref_field_types = REFERENCED_ENTITY_TYPES_BY_FIELD_BY_CLASS_NAME[model.entityType]

    mutable_values = model.model_dump(include=set(mutable_fields))
    final_values = model.model_dump(include=set(final_fields))
    all_values = {**mutable_values, **final_values}

    text_values = model.model_dump(include=set(text_fields))
    link_values = model.model_dump(include=set(link_fields))

    ref_values = model.model_dump(include=set(ref_fields))

    create_rels = []
    for node_label, raws in [
        (Text.__name__, text_values),
        (Link.__name__, link_values),
    ]:
        for edge_label, raw_values in to_key_and_values(raws):
            for position, raw_value in enumerate(raw_values):
                create_rels.append(
                    GraphRel(
                        edgeLabel=edge_label,
                        edgeProps={"position": position},
                        nodeLabels=[node_label],
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
                    nodeLabels=ref_field_types[field],
                    nodeProps={"identifier": identifier},
                )
            )

    return IngestData(
        identifier=None if isinstance(model, AnyRuleModel) else str(model.identifier),
        stableTargetId=stable_target_id,
        entityType=model.entityType,
        nodeProps=all_values,
        linkRels=link_rels,
        createRels=create_rels,
    )


# TODO(ND): move this to mex-common
def clean_dict(obj: Any) -> Any:  # noqa: ANN401
    """Clean `None` and `[]` from dicts."""
    if isinstance(obj, dict):
        cleaned = {}
        for k, v in obj.items():
            cleaned_value = clean_dict(v)
            if cleaned_value not in (None, []):
                cleaned[k] = cleaned_value
        return cleaned
    if isinstance(obj, list):
        return [clean_dict(item) for item in obj]
    return obj


def get_graph_rel_id(rel: GraphRel) -> tuple[str, int]:
    """Returns a string uniquely identifying the GraphRel."""
    return rel["edgeLabel"], cast("int", rel["edgeProps"]["position"])


def validate_ingested_data(
    data_in: IngestData, data_out: IngestData
) -> list[ErrorDetails]:
    """Validate that the ingestion has been executed successfully."""
    error_details = []
    for field in ("stableTargetId", "identifier", "entityType", "nodeProps"):
        value_in = clean_dict(getattr(data_in, field))
        value_out = clean_dict(getattr(data_out, field))
        if value_out != value_in:
            error_details.append(
                ErrorDetails(
                    type="transaction_failed",
                    msg="ingested data did not match expectation",
                    loc=(field,),
                    input=value_out,
                    ctx={"expected": value_in},
                )
            )
    for rel_field in ("linkRels", "createRels"):
        in_lookup = {
            get_graph_rel_id(rel): cast("GraphRel", rel)
            for rel in getattr(data_in, rel_field)
        }
        out_lookup = {
            get_graph_rel_id(rel): cast("GraphRel", rel)
            for rel in getattr(data_out, rel_field)
        }
        for rel_id, out_rel in out_lookup.items():
            in_rel = in_lookup.get(rel_id)
            if not in_rel:
                error_details.append(
                    ErrorDetails(
                        type="transaction_failed",
                        msg="ingestion would have created unexpected relation",
                        loc=(rel_field, *rel_id),
                        input=out_rel,
                        ctx={"expected": None},
                    )
                )
                continue
            if not set(out_rel["nodeLabels"]) <= set(in_rel["nodeLabels"]):
                error_details.append(
                    ErrorDetails(
                        type="transaction_failed",
                        msg="referenced node has unexpected labels",
                        loc=(rel_field, *rel_id, "nodeLabels"),
                        input=out_rel["nodeLabels"],
                        ctx={"expected": in_rel["nodeLabels"]},
                    )
                )
            if clean_dict(out_rel["nodeProps"]) != clean_dict(in_rel["nodeProps"]):
                error_details.append(
                    ErrorDetails(
                        type="transaction_failed",
                        msg="referenced node has unexpected properties",
                        loc=(rel_field, *rel_id, "nodeProps"),
                        input=out_rel["nodeProps"],
                        ctx={"expected": in_rel["nodeProps"]},
                    )
                )
        for rel_id, in_rel in in_lookup.items():
            if rel_id not in out_lookup:
                error_details.append(
                    ErrorDetails(
                        type="transaction_failed",
                        msg="ingestion failed to create expected relation",
                        loc=(rel_field, *rel_id),
                        input=None,
                        ctx={"expected": in_rel},
                    )
                )
    return error_details


def get_error_details_from_neo4j_error(
    input_: AnyExtractedModel
    | AnyRuleSetResponse
    | MExPrimarySource
    | AnyPrimitiveType,
    error: Neo4jError,
) -> list[ErrorDetails]:
    """Convert an input context and a neo4j error into error details."""
    return [
        ErrorDetails(
            type=error.code or "unknown",
            msg=error.message or "unknown",
            loc=(),
            input=input_,
            ctx={"meta": error.metadata},
        )
    ]
