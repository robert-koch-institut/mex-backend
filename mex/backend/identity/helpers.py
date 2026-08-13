from mex.backend.cache.connector import CacheConnector


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


def reset_identity_cache(moved_identities: list[dict[str, str]]) -> None:
    """Drop the cached identities of extracted items that changed merged item.

    The identity provider caches which stable target id a given provenance resolves to.
    After a merge that mapping is stale for every extracted item that was moved to the
    keeper, so the entries are dropped and the next lookup falls back to the graph.
    Without this, re-running an extractor would resurrect the merged-away item.

    Note that this only reaches other backend instances when a shared valkey cache is
    configured, which settings require whenever more than one instance runs.

    Args:
        moved_identities: Provenance of the extracted items that were moved, as
            returned by the `move_extracted_items` query
    """
    cache = CacheConnector.get()
    for identity in moved_identities:
        cache.delete_value(
            get_identity_cache_key(
                identity["hadPrimarySource"],
                identity["identifierInPrimarySource"],
            )
        )
