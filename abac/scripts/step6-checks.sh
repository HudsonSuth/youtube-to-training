#!/usr/bin/env bash
# Step 6: principal produce/consume, broker deny, Flink mask, sync after a tag change.
set -euo pipefail
cd "$(dirname "$0")/.."

if ! docker compose version >/dev/null 2>&1; then
  echo "docker compose is required" >&2
  exit 1
fi

TOKEN="ssn-$(date +%s)"
PAYLOAD="{\"ssn\":\"${TOKEN}\",\"amount\":4200}"

wait_healthy() {
  local service="$1"
  local tries="${2:-40}"
  local i status
  for ((i = 1; i <= tries; i++)); do
    status="$(docker compose ps --format '{{.Service}} {{.Health}}' | awk -v svc="$service" '$1 == svc { print $2 }')"
    if [[ "$status" == "healthy" ]]; then
      return 0
    fi
    sleep 3
  done
  echo "${service} did not become healthy (last status: ${status:-unknown})" >&2
  docker compose ps
  return 1
}

cypher() {
  docker compose exec -T neo4j cypher-shell -a bolt://localhost:7687 -u neo4j -p abac-local "$1"
}

produce() {
  printf '%s\n' "$PAYLOAD" | docker compose exec -T kafka \
    /opt/kafka/bin/kafka-console-producer.sh \
    --bootstrap-server kafka:9092 \
    --topic credit-scores \
    --producer.config /mnt/shared/config/client-pii-consumer.properties
}

consume() {
  local config="$1"
  local topic="$2"
  local timeout_ms="$3"
  local max_messages="$4"
  docker compose exec -T kafka \
    /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server kafka:9092 \
    --topic "$topic" \
    --from-beginning \
    --timeout-ms "$timeout_ms" \
    --max-messages "$max_messages" \
    --consumer.config "/mnt/shared/config/${config}" \
    2>&1 || true
}

opa_get() {
  docker compose exec -T -e OPA_PATH="$1" sync \
    python -c 'import os,urllib.request; print(urllib.request.urlopen("http://opa:8181"+os.environ["OPA_PATH"], timeout=10).read().decode(), end="")'
}

opa_allow() {
  docker compose exec -T \
    -e OPA_PRINCIPAL="$1" \
    -e OPA_TOPIC="$2" \
    -e OPA_OPERATION="${3:-READ}" \
    sync python -c 'import json,os,urllib.request
payload={"input":{"action":{"operation":os.environ["OPA_OPERATION"],"resourcePattern":{"name":os.environ["OPA_TOPIC"],"resourceType":"TOPIC","patternType":"LITERAL"}},"requestContext":{"principal":{"name":os.environ["OPA_PRINCIPAL"],"principalType":"User"}}}}
req=urllib.request.Request("http://opa:8181/v1/data/kafka/authz/allow", data=json.dumps(payload).encode(), headers={"Content-Type":"application/json"})
print(urllib.request.urlopen(req, timeout=10).read().decode(), end="")'
}

echo "1. Restore the pii tag and wait until the sync publishes both principals"
cypher "MERGE (t:Topic {name: 'credit-scores-sanitized'}) SET t.region = 'us', t.classification = 1"
cypher "MATCH (t:Topic {name: 'credit-scores'}) MATCH (pii:Tag {name: 'pii'}) MERGE (t)-[:HAS_TAG]->(pii)"
wait_healthy sync
for ((i = 1; i <= 20; i++)); do
  data="$(opa_get /v1/data)"
  if python3 -c 'import json,sys; doc=json.loads(sys.argv[1])["result"]; tags=doc["topics"]["credit-scores"]["tags"]; principals=doc["principals"]; raise SystemExit(0 if "pii" in tags and "credit-scores-sanitized" in doc["topics"] and {"analyst","pii_consumer"} <= set(principals) else 1)' "$data"; then
    break
  fi
  if [[ "$i" -eq 20 ]]; then
    echo "OPA projection did not include the pii tag and both principals" >&2
    echo "$data" >&2
    exit 1
  fi
  sleep 3
done
echo "   projection ready"

sanitized_end_offset() {
  local line
  line="$(docker compose exec -T kafka /opt/kafka/bin/kafka-get-offsets.sh \
    --bootstrap-server kafka:9092 \
    --topic credit-scores-sanitized \
    --command-config /mnt/shared/config/client-kafka.properties 2>/dev/null || true)"
  python3 -c 'import sys
text=sys.argv[1].strip().splitlines()
offsets=[int(row.split(":")[-1]) for row in text if row.count(":") >= 2 and row.split(":")[-1].lstrip("-").isdigit()]
print(max(offsets) if offsets else 0)' "$line"
}

echo "2. Produce a credit-scores record as pii_consumer"
before_sanitized="$(sanitized_end_offset)"
produce
echo "   produced ${PAYLOAD}"

echo "3. OPA denies analyst and allows pii_consumer on the PII topic"
analyst_decision="$(opa_allow analyst credit-scores)"
consumer_decision="$(opa_allow pii_consumer credit-scores)"
missing_principal="$(opa_allow nobody credit-scores)"
missing_topic="$(opa_allow analyst missing-topic)"
write_decision="$(opa_allow analyst credit-scores WRITE)"
python3 -c 'import json,sys
assert json.loads(sys.argv[1])["result"] is False
assert json.loads(sys.argv[2])["result"] is True
assert json.loads(sys.argv[3])["result"] is False
assert json.loads(sys.argv[4])["result"] is False
assert json.loads(sys.argv[5])["result"] is True' \
  "$analyst_decision" "$consumer_decision" "$missing_principal" "$missing_topic" "$write_decision"
echo "   analyst=${analyst_decision} pii_consumer=${consumer_decision} missing_principal=${missing_principal} missing_topic=${missing_topic} write=${write_decision}"

echo "4. Broker denies the low-clearance analyst read"
denied="$(consume client-analyst.properties credit-scores 15000 1)"
if [[ "$denied" == *"$TOKEN"* ]]; then
  echo "analyst was able to read the PII topic" >&2
  echo "$denied" >&2
  exit 1
fi
if [[ "$denied" != *uthorized* && "$denied" != *uthorization* ]]; then
  echo "expected a broker authorization failure" >&2
  echo "$denied" >&2
  exit 1
fi
echo "   broker denied analyst"

echo "5. pii_consumer can read the raw record"
raw="$(consume client-pii-consumer.properties credit-scores 20000 20)"
if [[ "$raw" != *"$TOKEN"* ]]; then
  echo "pii_consumer did not see the raw record" >&2
  echo "$raw" >&2
  exit 1
fi
echo "   pii_consumer saw the raw ssn"

echo "6. Flink masks high and unknown fields for analyst"
masked=""
for ((i = 1; i <= 24; i++)); do
  now="$(sanitized_end_offset)"
  if [[ "$now" -gt "$before_sanitized" ]]; then
    masked="$(docker compose exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh \
      --bootstrap-server kafka:9092 \
      --topic credit-scores-sanitized \
      --partition 0 \
      --offset "$before_sanitized" \
      --timeout-ms 8000 \
      --max-messages 1 \
      --consumer.config /mnt/shared/config/client-analyst.properties 2>&1 || true)"
    if [[ "$masked" == *"***MASKED***"* && "$masked" != *"$TOKEN"* && "$masked" != *"4200"* ]]; then
      break
    fi
  fi
  if [[ "$i" -eq 24 ]]; then
    echo "sanitized topic did not mask the new record" >&2
    echo "$masked" >&2
    docker compose logs --tail 80 flink >&2 || true
    exit 1
  fi
  sleep 5
done
echo "   sanitized record masks ssn and the unknown amount"

echo "7. Remove the pii tag and wait for the next sync"
cypher "MATCH (t:Topic {name: 'credit-scores'})-[r:HAS_TAG]->(:Tag {name: 'pii'}) DELETE r"
for ((i = 1; i <= 20; i++)); do
  data="$(opa_get /v1/data/topics/credit-scores)"
  decision="$(opa_allow analyst credit-scores)"
  if python3 -c 'import json,sys; tags=json.loads(sys.argv[1]).get("result",{}).get("tags",[]); decision=json.loads(sys.argv[2]); raise SystemExit(0 if "pii" not in tags and decision.get("result") is True else 1)' "$data" "$decision"; then
    echo "   OPA now allows analyst: ${decision}"
    break
  fi
  if [[ "$i" -eq 20 ]]; then
    echo "tag change was not reflected in OPA" >&2
    echo "data=${data}" >&2
    echo "decision=${decision}" >&2
    exit 1
  fi
  sleep 3
done

echo "8. Restart the broker so the 300s decision cache drops, then read as analyst"
docker compose restart kafka >/dev/null
wait_healthy kafka 40
allowed="$(consume client-analyst.properties credit-scores 20000 20)"
if [[ "$allowed" != *"$TOKEN"* ]]; then
  echo "analyst still cannot read credit-scores after the tag sync" >&2
  echo "$allowed" >&2
  exit 1
fi
echo "   broker allows analyst after the sync"
echo "PASS"
