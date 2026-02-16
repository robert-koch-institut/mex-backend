from typing import TYPE_CHECKING

from mex.backend.extracted.helpers import search_extracted_items_in_graph
from mex.common.identity import get_provider

if TYPE_CHECKING:  # pragma: no cover
    from mex.common.models import AnyExtractedModel
    from mex.common.types import MergedPrimarySourceIdentifier


def fetch_extracted_item_by_source_identifiers(
    had_primary_source: MergedPrimarySourceIdentifier,
    identifier_in_primary_source: str,
) -> AnyExtractedModel | None:
    """Fetch an extracted item by source identifiers."""
    provider = get_provider()
    identities = provider.fetch(
        had_primary_source=had_primary_source,
        identifier_in_primary_source=identifier_in_primary_source,
    )
    if identities:
        container = search_extracted_items_in_graph(
            stable_target_id=identities[0].stableTargetId
        )
        if container.items:
            return container.items[0]
    return None
