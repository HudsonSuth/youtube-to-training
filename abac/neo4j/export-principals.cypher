MATCH (p:Principal)
RETURN p.name AS name,
       p.clearance AS clearance,
       p.region AS region
