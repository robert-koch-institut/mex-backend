<# Collect each merged item's effective vocabulary values, per field.

Folds additive and subtractive rule semantics: a value counts for an item when
at least one extracted or additive component carries it and no subtractive
component removes it. Counting a subtractive value as positive evidence would
be sign flipped, because those are exactly the values a curator has decided do
not belong on the item.

This deliberately does not use `any_extracted_or_rule_label`, which includes the
subtractive labels. Preventive components need no handling here: their
vocabulary named fields are typed as primary source identifiers, so they are
stored as relationships and can never surface as a node property.

Preventive rules and primary source precedence are knowingly ignored, so these
counts approximate the merged truth. That is acceptable for a prior over what
comparable records look like, but do not reuse them where a fact is required.

Globals:
    any_extracted_label: pipe delimited sequence of all extracted class labels
    any_additive_label: pipe delimited sequence of all additive class labels
    any_subtractive_label: pipe delimited sequence of all subtractive class labels

Jinja:
    carry: Already bound variables to thread through, comma terminated

Args:
    merged_node: The bound merged node whose components to inspect
    vocabulary_fields: List of vocabulary field names to collect
    subtractive_labels: List of labels whose values are removals

Returns:
    One row per (carry..., merged_node, field) with at least one effective
    value, where `values_on_item` holds the deduplicated values
-#>
<% from "macros/normalize_vocabulary_values.cql" import normalize_vocabulary_values with context -%>
<% macro collect_effective_vocabulary_values(carry="") -%>
MATCH (component:<<any_extracted_label>>|<<any_additive_label>>|<<any_subtractive_label>>)-[:stableTargetId]->(merged_node)
UNWIND $vocabulary_fields AS field
WITH
    <<carry>>merged_node,
    field,
    head(labels(component)) IN $subtractive_labels AS is_negative,
    <<normalize_vocabulary_values("component", "field")|indent(4)>> AS values_on_component
UNWIND values_on_component AS value
WITH
    <<carry>>merged_node,
    field,
    value,
    max(CASE WHEN is_negative THEN 0 ELSE 1 END) AS added,
    max(CASE WHEN is_negative THEN 1 ELSE 0 END) AS removed
WHERE added = 1 AND removed = 0
WITH <<carry>>merged_node, field, collect(value) AS values_on_item
<%- endmacro %>
