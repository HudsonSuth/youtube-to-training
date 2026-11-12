# Graph-backed ABAC for Kafka

Neo4j is the source of truth for topic tags, field sensitivity, and principal clearance. OPA is the policy decision point. The OPA Kafka authorizer is the broker policy enforcement point. A Java 17 Flink job masks high-sensitivity fields onto a downstream topic.

OPA data is a projection. The sync job replaces it from Neo4j. A failed sync leaves the previous projection in place.

## Prerequisites

- Docker with Compose v2
- Kafka 3.8+ (this stack uses `apache/kafka:3.8.1`)
- Java 17 (the Flink and Kafka images supply it; you do not need a host JDK to run the stack)
- Neo4j 5 (`neo4j:5.26.8`)
- OPA (`openpolicyagent/opa:1.4.2`)

Local SASL passwords below are for this compose network only.

| Principal | SASL password | Clearance | Region |
| --- | --- | --- | --- |
| `pii_consumer` | `pii-secret` | 3 | us |
| `analyst` | `analyst-secret` | 1 | eu |
| `kafka` (broker only, super user) | `kafka-secret` | — | — |

Neo4j auth is `neo4j` / `abac-local`.

## Layout

- `neo4j/init.cypher` loads topics, fields, tags, and both principals on startup
- `neo4j/export-topics.cypher` is the topic-metadata query
- `neo4j/export-principals.cypher` exports every principal, including `analyst`
- `policies/kafka.authz.rego` is package `kafka.authz` (default deny, then allow when the PII read rule does not match)
- `sync/sync.py` runs those queries and `PUT`s `/v1/data`
- `flink/` masks `ssn` when the audience clearance is below 3

## Bring-up order

Compose starts the services in this order:

1. **Graph.** Neo4j becomes healthy, then `neo4j-init` loads `init.cypher`.
2. **Rego.** OPA serves `policies/kafka.authz.rego`.
3. **Sync.** The sync job publishes `topics` and `principals` before it is marked healthy. Kafka waits for that.
4. **Plugin.** The Kafka image puts `opa-authorizer-1.5.1.jar` (plus Guava, which that jar does not embed) on the classpath from `/usr/local/share/kafka/plugins/` and sets `authorizer.class.name=org.openpolicyagent.kafka.OpaAuthorizer`.
5. **Flink.** The mask job starts after the broker is healthy. It reads `credit-scores` as `pii_consumer` and writes `credit-scores-sanitized` for `analyst`.
6. **Hardening**, already set on the broker:
   - `opa.authorizer.url=http://opa:8181/v1/data/kafka/authz/allow`
   - `opa.authorizer.allow.on.error=false`
   - `opa.authorizer.cache.initial.capacity=5000`
   - `opa.authorizer.cache.maximum.size=50000`
   - `opa.authorizer.cache.expire.after.seconds=300`
   - `super.users=User:kafka` so the broker identity is not fail-closed out of the cluster. `pii_consumer` and `analyst` are not super users.

Clients inside the compose network use bootstrap `kafka:9092`. The advertised listener is that Docker DNS name, so produce and consume with `docker compose exec` rather than a host Kafka client.

## Start

```bash
cd abac
docker compose up --build -d
docker compose ps
```

`sync`, `kafka`, and `neo4j` should report healthy. The first Kafka start waits until the projection contains both principals.

## Step 6 checks

`./scripts/step6-checks.sh` runs the flow below. The manual commands are the same checks.

```bash
cd abac
./scripts/step6-checks.sh
```

### Produce and consume as different principals

```bash
printf '%s\n' '{"ssn":"123-45-6789","amount":4200}' | docker compose exec -T kafka \
  /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server kafka:9092 \
  --topic credit-scores \
  --producer.config /mnt/shared/config/client-pii-consumer.properties

docker compose exec -T kafka \
  /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 \
  --topic credit-scores \
  --from-beginning \
  --max-messages 1 \
  --timeout-ms 15000 \
  --consumer.config /mnt/shared/config/client-pii-consumer.properties
```

`pii_consumer` (clearance 3) sees the raw `ssn`.

### Low-clearance deny at the broker

```bash
docker compose exec -T kafka \
  /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 \
  --topic credit-scores \
  --from-beginning \
  --max-messages 1 \
  --timeout-ms 15000 \
  --consumer.config /mnt/shared/config/client-analyst.properties
```

`analyst` (clearance 1) is denied on `credit-scores` because the topic is tagged `pii` and clearance is below the topic classification. The same read against OPA:

```bash
curl -s -X POST http://127.0.0.1:8181/v1/data/kafka/authz/allow \
  -H 'content-type: application/json' \
  -d '{"input":{"action":{"operation":"READ","resourcePattern":{"name":"credit-scores","resourceType":"TOPIC","patternType":"LITERAL"}},"requestContext":{"principal":{"name":"analyst","principalType":"User"}}}}'
```

That returns `{"result":false}`. `pii_consumer` returns `{"result":true}`.

### Flink masking

The job reads raw `credit-scores` and writes `credit-scores-sanitized`. For `analyst`, `ssn` is `***MASKED***` and `amount` is unchanged. `analyst` is allowed to read the sanitized topic because that topic is not tagged `pii`.

```bash
docker compose exec -T kafka \
  /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 \
  --topic credit-scores-sanitized \
  --from-beginning \
  --max-messages 5 \
  --timeout-ms 20000 \
  --consumer.config /mnt/shared/config/client-analyst.properties
```

### Tag change after sync

The broker caches an allow or deny for 300 seconds. OPA itself updates on the next sync (every 15 seconds).

```bash
docker compose exec -T neo4j cypher-shell -a bolt://localhost:7687 -u neo4j -p abac-local \
  "MATCH (t:Topic {name: 'credit-scores'})-[r:HAS_TAG]->(:Tag {name: 'pii'}) DELETE r"
```

Wait for a sync cycle, then confirm the projection and the decision:

```bash
curl -s http://127.0.0.1:8181/v1/data/topics/credit-scores
curl -s -X POST http://127.0.0.1:8181/v1/data/kafka/authz/allow \
  -H 'content-type: application/json' \
  -d '{"input":{"action":{"operation":"READ","resourcePattern":{"name":"credit-scores","resourceType":"TOPIC","patternType":"LITERAL"}},"requestContext":{"principal":{"name":"analyst","principalType":"User"}}}}'
```

`tags` no longer contains `pii`, and the decision is `{"result":true}`.

A broker that already denied `analyst` keeps that cached decision until the 300 second TTL. Restart the broker to drop the cache without waiting:

```bash
docker compose restart kafka
```

After it is healthy, the analyst consume command above returns the raw record.

Restore the tag with:

```bash
docker compose exec -T neo4j cypher-shell -a bolt://localhost:7687 -u neo4j -p abac-local \
  "MATCH (t:Topic {name: 'credit-scores'}) MATCH (pii:Tag {name: 'pii'}) MERGE (t)-[:HAS_TAG]->(pii)"
```

## Stop

```bash
docker compose down
```

`docker compose down -v` also drops the Neo4j volume. The next start loads `init.cypher` again.
