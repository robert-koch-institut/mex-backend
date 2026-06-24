from typing import Final

# Sentinel value used in reference filters to match merged items where *no*
# component has an outgoing relationship for a given field (universal absence,
# e.g. items without any contact at all).
NO_REFERENCE_SENTINEL: Final = "__NO_REF__"
