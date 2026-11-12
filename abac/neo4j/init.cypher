// Topics, fields, tags, and principals from the architecture spec.
// Each statement stands alone so cypher-shell can replay this file on startup.
CREATE CONSTRAINT topic_name IF NOT EXISTS FOR (t:Topic) REQUIRE t.name IS UNIQUE;
CREATE CONSTRAINT field_name IF NOT EXISTS FOR (f:Field) REQUIRE f.name IS UNIQUE;
CREATE CONSTRAINT tag_name IF NOT EXISTS FOR (g:Tag) REQUIRE g.name IS UNIQUE;
CREATE CONSTRAINT principal_name IF NOT EXISTS FOR (p:Principal) REQUIRE p.name IS UNIQUE;

MERGE (t:Topic {name: "credit-scores"})
SET t.region = "us", t.classification = 3;

MERGE (t:Topic {name: "orders"})
SET t.region = "eu", t.classification = 1;

MERGE (t:Topic {name: "credit-scores-sanitized"})
SET t.region = "us", t.classification = 1;

MERGE (f:Field {name: "ssn"})
SET f.sensitivity = "high";

MERGE (f:Field {name: "amount"})
SET f.sensitivity = "low";

MATCH (t:Topic {name: "credit-scores"})
MATCH (f:Field {name: "ssn"})
MERGE (t)-[:HAS_FIELD]->(f);

MATCH (t:Topic {name: "orders"})
MATCH (f:Field {name: "amount"})
MERGE (t)-[:HAS_FIELD]->(f);

MERGE (pii:Tag {name: "pii"});

MATCH (t:Topic {name: "credit-scores"})
MATCH (pii:Tag {name: "pii"})
MERGE (t)-[:HAS_TAG]->(pii);

MERGE (p:Principal {name: "pii_consumer"})
SET p.clearance = 3, p.region = "us";

MERGE (p:Principal {name: "analyst"})
SET p.clearance = 1, p.region = "eu";
