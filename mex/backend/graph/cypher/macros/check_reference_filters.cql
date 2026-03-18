<# Filter nodes by their outgoing references matching specific identifiers.

For each reference field, collect existing references and fill in a sentinel for
missing fields, then assert all reference filters are satisfied.

Globals:
    any_merged_label: List of all merged class labels
    no_reference: Sentinel value for missing references

Context:
    reference_fields: List of relationship names to check

Macro args:
    source_node: Name of the node variable to match references from
    group_vars: Comma-separated variables for the first WITH clause
    carry_vars: Comma-separated variables to carry through the second WITH clause,
                empty string omits extra variables
-#>
<% macro check_reference_filters(source_node, group_vars, carry_vars="") -%>
OPTIONAL MATCH (<<source_node>>)-[reference:<<reference_fields|join("|")>>]->(ref_target:<<any_merged_label>>)
WITH <<group_vars>>,
    collect(CASE WHEN reference IS NOT NULL THEN type(reference) END) AS found_fields,
    collect(CASE WHEN reference IS NOT NULL
        THEN {field: type(reference), identifier: ref_target.identifier} END
    ) AS existing_refs
WITH<% if carry_vars %> <<carry_vars>>,<% endif %>
    existing_refs +
    [f IN $reference_fields WHERE NOT f IN found_fields | {field: f, identifier: "<<no_reference>>"}]
    AS ref_matches
WHERE ALL(rf IN $reference_filters WHERE
    ANY(m IN ref_matches WHERE m.field = rf.field AND m.identifier IN rf.identifiers)
)
<%- endmacro %>
