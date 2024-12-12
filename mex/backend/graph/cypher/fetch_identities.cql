<# Fetch only identity-related fields for the given set of filters.

Globals:
    extracted_labels: List of all extracted class labels
    merged_labels: List of all merged class labels

Args:
    filter_by_had_primary_source: Whether the final query should filter by identifiers
                                  of MergedPrimarySources referenced by hadPrimarySource
    filter_by_identifier_in_primary_source: Whether the final query should filter by
                                            the value of identifierInPrimarySource
    filter_by_stable_target_id: Whether the final query should filter by stableTargetId

Returns:
    List of identity objects.
-#>
MATCH (n:<<extracted_labels|join("|")>>)-[:stableTargetId]->(merged:<<merged_labels|join("|")>>)
MATCH (n)-[:hadPrimarySource]->(primary_source:MergedPrimarySource)
<%- if filter_by_had_primary_source or filter_by_identifier_in_primary_source or filter_by_stable_target_id %>
WHERE
    <%- set and_ = joiner("AND ") -%>
    <%- if filter_by_had_primary_source %>
    <<and_()>>primary_source.identifier = $had_primary_source
    <%- endif %>
    <%- if filter_by_identifier_in_primary_source %>
    <<and_()>>n.identifierInPrimarySource = $identifier_in_primary_source
    <%- endif -%>
    <%- if filter_by_stable_target_id %>
    <<and_()>>merged.identifier = $stable_target_id
    <%- endif -%>
<%- endif %>
RETURN
    merged.identifier AS stableTargetId,
    primary_source.identifier AS hadPrimarySource,
    n.identifierInPrimarySource AS identifierInPrimarySource,
    n.identifier AS identifier
ORDER BY n.identifier ASC
LIMIT $limit;
