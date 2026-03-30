<# Collect referenced identifiers and nested objects for an extracted or rule node.

Returns a list of maps with value, position, and label for each outgoing
relationship to a merged node (identifier reference) or nested node (properties).

Globals:
    any_merged_label: List of all merged class labels
    any_nested_label: List of labels for all nestable objects

Args:
    extracted_or_rule_node: The node whose outgoing references and nested objects to collect
-#>
<% macro collect_references_and_nested() -%>
[
    (extracted_or_rule_node)-[r]->(referenced_merged_node:<<any_merged_label>>) |
    {value: referenced_merged_node.identifier, position:r.position, label: type(r)}
] + [
    (extracted_or_rule_node)-[r]->(referenced_nested_node:<<any_nested_label>>) |
    {value: properties(referenced_nested_node), position:r.position , label: type(r)}
]
<%- endmacro %>
