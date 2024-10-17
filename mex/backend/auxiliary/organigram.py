from mex.common.models import ExtractedOrganizationalUnit, ExtractedPrimarySource
from mex.common.organigram.extract import (
    extract_organigram_units,
    get_unit_merged_ids_by_synonyms,
)
from mex.common.organigram.transform import (
    transform_organigram_units_to_organizational_units,
)
from mex.common.types import MergedOrganizationalUnitIdentifier
from mex.extractors.pipeline import asset  # type: ignore
from mex.extractors.sinks import load  # type: ignore


@asset(group_name="default")  # type: ignore
def extracted_organizational_units(
    extracted_primary_source_organigram: ExtractedPrimarySource,
) -> list[ExtractedOrganizationalUnit]:
    """Extract organizational units."""
    organigram_units = extract_organigram_units()
    mex_organizational_units = list(
        transform_organigram_units_to_organizational_units(
            organigram_units,
            extracted_primary_source_organigram,
        )
    )
    load(mex_organizational_units)
    return mex_organizational_units


@asset(group_name="default")  # type: ignore
def unit_stable_target_ids_by_synonym(
    extracted_organizational_units: list[ExtractedOrganizationalUnit],
) -> dict[str, MergedOrganizationalUnitIdentifier]:
    """Group organizational units by their synonym."""
    return {
        synonym: MergedOrganizationalUnitIdentifier(merged_id)
        for synonym, merged_id in get_unit_merged_ids_by_synonyms(
            extracted_organizational_units
        ).items()
    }
