CREATE INDEX nested_text IF NOT EXISTS FOR (n:Text) ON (n.value, n.language);
CREATE INDEX nested_link IF NOT EXISTS FOR (n:Link) ON (n.title, n.url, n.language);
