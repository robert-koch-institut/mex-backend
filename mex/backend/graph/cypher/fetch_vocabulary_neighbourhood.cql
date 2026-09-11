<# Fetch the top scoring merged items for a query string, with their values.

Unlike the cohort aggregation, this returns raw per item evidence rather than
counts. The neighbourhood is capped at `neighbour_limit` items, so the payload
stays small, and keeping the score weighted vote in python makes the blending
maths unit testable without a database.

Jinja:
    filter_by_exclusion: Whether to exclude one merged item from the results

Args:
    query_string: Escaped lucene query string
    hit_limit: Cap on raw hits requested from the index
    labels: Labels to restrict the neighbourhood to
    exclude_identifier: Identifier of a merged item to leave out
    neighbour_limit: Cap on merged items allowed to vote
    vocabulary_fields: Vocabulary field names to collect
    subtractive_labels: Labels whose values are removals

Returns:
    neighbours: List of merged items, highest scoring first, each with:
        identifier: Identifier of the merged item
        score: Lucene relevance score
        fields: List of effective values per non empty vocabulary field
-#>
<% from "macros/search_nodes_with_score.cql" import search_nodes_with_score with context -%>
<% from "macros/collect_effective_vocabulary_values.cql" import collect_effective_vocabulary_values with context -%>

<<search_nodes_with_score()>>
WHERE
    ANY(label IN labels(merged_node) WHERE label IN $labels)
    <%- if filter_by_exclusion %>
    AND merged_node.identifier <> $exclude_identifier
    <%- endif %>
ORDER BY score DESC, merged_node.identifier ASC
LIMIT $neighbour_limit
<<collect_effective_vocabulary_values(carry="score, ")>>
WITH merged_node, score, collect({field: field, values: values_on_item}) AS fields
ORDER BY score DESC, merged_node.identifier ASC
RETURN collect({
    identifier: merged_node.identifier, score: score, fields: fields
}) AS neighbours;
