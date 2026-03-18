<# Filter nodes by their outgoing references matching specific identifiers.

For each reference field, collect existing references and fill in a sentinel for
missing fields, then assert all reference filters are satisfied.

Globals:
    any_merged_label: List of all merged class labels
    no_reference: Sentinel value for missing references

Jinja:
    reference_fields: List of relationship names to check

Args:
    extracted_or_rule_node: The node whose outgoing references to check
    merged_node: The merged node associated with the extracted or rule node
-#>
<% macro check_reference_filters() -%>
OPTIONAL MATCH (extracted_or_rule_node)-[reference:<<reference_fields|join("|")>>]->(referenced_merged_node:<<any_merged_label>>)
WITH
    merged_node,
    extracted_or_rule_node,
    collect(CASE WHEN reference IS NOT NULL THEN type(reference) END) AS found_fields,
    collect(CASE WHEN reference IS NOT NULL
        THEN {field: type(reference), identifier: referenced_merged_node.identifier} END
    ) AS existing_refs
WITH
    merged_node,
    extracted_or_rule_node,
    existing_refs +
    [f IN $reference_fields WHERE NOT f IN found_fields | {field: f, identifier: "<<no_reference>>"}]
    AS ref_matches
WHERE ALL(rf IN $reference_filters WHERE
    ANY(m IN ref_matches WHERE m.field = rf.field AND m.identifier IN rf.identifiers)
)
<%- endmacro %>
