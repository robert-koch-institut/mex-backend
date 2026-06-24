from typing import Final

# Sentinel value used in reference filters to match nodes that have no outgoing
# relationship for a given field (e.g. items without a hadPrimarySource edge).
NO_REFERENCE_SENTINEL: Final = "__NO_REF__"
