<# Aggregate the distribution of vocabulary values over a set of merged items.

Counts DISTINCT merged items, never raw components, so an item whose extracted
and additive components both carry the same value contributes one observation.

Globals:
    any_merged_label: pipe delimited sequence of all merged class labels

Jinja:
    filter_by_references: Whether to restrict the cohort by reference filters
    filter_by_exclusion: Whether to exclude one merged item from the cohort
    reference_fields: List of relationship names holding referenced identifiers

Args:
    labels: Labels to restrict the cohort to
    reference_filters: List of reference filters
    reference_fields: List of reference field names
    exclude_identifier: Identifier of a merged item to leave out of the cohort
    vocabulary_fields: Vocabulary field names to aggregate over
    subtractive_labels: Labels whose values are removals
    top_k: Keep at most this many values per field

Returns:
    cohortSize: Count of all merged items in the cohort
    aggregations: List of per field distributions, each with the attributes:
        field: Name of the vocabulary field
        fieldSupport: Count of cohort items with at least one effective value
        candidates: Values with their item counts, most frequent first
-#>
<% from "macros/filter_nodes_by_reference_filters.cql" import filter_nodes_by_reference_filters with context -%>
<% from "macros/collect_effective_vocabulary_values.cql" import collect_effective_vocabulary_values with context -%>

CALL () {
    <% block match_clause -%>
    MATCH (merged_node:<<any_merged_label>>)
    WHERE
        ANY(label IN labels(merged_node) WHERE label IN $labels)
        <%- if filter_by_references %>
        AND EXISTS {
            <<filter_nodes_by_reference_filters()|indent(12)>>
        }
        <%- endif %>
        <%- if filter_by_exclusion %>
        AND merged_node.identifier <> $exclude_identifier
        <%- endif %>
    <%- endblock %>
    RETURN COUNT(DISTINCT merged_node) AS cohortSize
}
CALL () {
    <<self.match_clause()>>
    <<collect_effective_vocabulary_values()|indent(4)>>
    WITH field, count(merged_node) AS field_support, collect(values_on_item) AS value_lists
    UNWIND value_lists AS values_on_item
    UNWIND values_on_item AS value
    WITH field, field_support, value, count(*) AS support
    ORDER BY field ASC, support DESC, value ASC
    WITH field, field_support, collect({value: value, support: support})[0..$top_k] AS candidates
    RETURN collect({
        field: field, fieldSupport: field_support, candidates: candidates
    }) AS aggregations
}
RETURN cohortSize, aggregations;
