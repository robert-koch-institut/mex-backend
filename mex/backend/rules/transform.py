def entity_type_to_merged_entity_type(entity_type: str) -> str:
    """Get the merged entity type for a given derivative entity type."""
    # XXX: maybe hardcode the "stem type" as a property in `mex-common`?
    stem_entity_type = (
        entity_type.removeprefix("Base")
        .removeprefix("Extracted")
        .removeprefix("Merged")
        .removeprefix("Additive")
        .removeprefix("Preventive")
        .removeprefix("Subtractive")
    )
    return f"Merged{stem_entity_type}"
