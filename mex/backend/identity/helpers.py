def get_identity_cache_key(
    had_primary_source: str,
    identifier_in_primary_source: str,
) -> str:
    """Build the cache key an identity is stored under.

    Args:
        had_primary_source: Identifier of the primary source the item belongs to
        identifier_in_primary_source: The identifier within the primary source

    Returns:
        Cache key for the given provenance
    """
    # newline is a safe delimiter because it is explicitly forbidden in both fields
    return f"{had_primary_source}\n{identifier_in_primary_source}"
