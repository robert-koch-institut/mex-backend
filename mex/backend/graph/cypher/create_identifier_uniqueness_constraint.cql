<# Create uniqueness constraints on identifier and provenance fields for all node.

Globals:
    extracted_labels: Labels of extracted item nodes
    merged_labels: Labels of merged item nodes
-#>
<% for extracted_label in extracted_labels -%>
CREATE CONSTRAINT <<extracted_label|dromedary_to_snake>>_identifier_uniqueness IF NOT EXISTS
FOR (extracted_node:<<extracted_label>>)
REQUIRE extracted_node.identifier IS UNIQUE;
CREATE CONSTRAINT <<extracted_label|dromedary_to_snake>>_provenance_uniqueness IF NOT EXISTS
FOR (extracted_node:<<extracted_label>>)
REQUIRE (extracted_node.hadPrimarySource, extracted_node.identifierInPrimarySource) IS UNIQUE;
<%- endfor %>
<% for merged_label in merged_labels -%>
CREATE CONSTRAINT <<merged_label|dromedary_to_snake>>_identifier_uniqueness IF NOT EXISTS
FOR (merged_node:<<merged_label>>)
REQUIRE merged_node.identifier IS UNIQUE;
CREATE CONSTRAINT <<merged_label|dromedary_to_snake>>_provenance_uniqueness IF NOT EXISTS
FOR (merged_node:<<merged_label>>)
REQUIRE (merged_node.hadPrimarySource, merged_node.identifierInPrimarySource) IS UNIQUE;
<%- endfor %>
