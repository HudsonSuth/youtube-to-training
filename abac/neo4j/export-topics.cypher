MATCH (t:Topic)
OPTIONAL MATCH (t)-[:HAS_TAG]->(tag)
RETURN t.name AS name,
       t.region AS region,
       t.classification AS classification,
       collect(tag.name) AS tags
