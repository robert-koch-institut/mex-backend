CREATE CONSTRAINT <<node_label|dromedary_to_snake>>_identifier_uniqueness IF NOT EXISTS
FOR (n:<<node_label>>)
REQUIRE n.identifier IS UNIQUE;
