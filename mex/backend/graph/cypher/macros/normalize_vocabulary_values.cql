<# Normalize a vocabulary field value into a list, whatever its cardinality.

Vocabulary values are stored as plain node properties, never as relationships.
Depending on the model variant, the same logical field is a scalar on some
classes (`AccessRestriction | None`) and a list on others (`list[Theme]`), so
callers cannot know which shape to expect for a dynamically chosen field.

Note that `null IS :: LIST<ANY>` evaluates to true, because cypher list types
are nullable, so the null case has to be checked before the list case.

Args:
    node: Name of the bound node variable
    field: Name of the bound field name variable

Returns:
    A cypher expression evaluating to a possibly empty list of values
-#>
<% macro normalize_vocabulary_values(node, field) -%>
CASE
    WHEN <<node>>[<<field>>] IS NULL THEN []
    WHEN <<node>>[<<field>>] IS :: LIST<ANY> THEN <<node>>[<<field>>]
    ELSE [<<node>>[<<field>>]]
END
<%- endmacro %>
