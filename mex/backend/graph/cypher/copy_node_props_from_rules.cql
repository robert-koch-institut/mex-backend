<# Copy node properties from an old rule to a new rule.

This query copies all properties from an old rule item to a new rule item.
Since rule properties are lists, the values are extended rather than replaced.

Globals:
    rule_labels: List of all rule class labels

Jinja:
    field_names: List of field names to copy from the old rule to the new rule

Args:
    old_rule_identifier: Identifier of the old rule item to copy properties from
    new_rule_identifier: Identifier of the new rule item to copy properties to

Returns:
    copied_properties_count: Number of properties that were copied
-#>
MATCH (old_rule:<<rule_labels|join("|")>> {identifier: $old_rule_identifier})
MATCH (new_rule:<<rule_labels|join("|")>> {identifier: $new_rule_identifier})
<%- for field_name in field_names %>
SET new_rule.<<field_name>> = CASE
    WHEN new_rule.<<field_name>> IS NULL THEN old_rule.<<field_name>>
    ELSE new_rule.<<field_name>> + old_rule.<<field_name>>
END
<%- endfor %>
RETURN <<field_names|length>> AS copied_properties_count;
