#!/usr/bin/env python3
"""Project Neo4j topics and principals into OPA.

Neo4j stays the source of truth. A failed read does not publish, so the
previous OPA document is left in place.
"""

import json
import os
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

READY_PATH = Path("/tmp/sync-ready")


def build_document(topic_rows, principal_rows):
    topics = {}
    for row in topic_rows:
        name = row.get("name")
        if not name:
            continue
        tags = [tag for tag in (row.get("tags") or []) if tag]
        topics[name] = {
            "region": row.get("region"),
            "classification": int(row["classification"]),
            "tags": tags,
        }

    principals = {}
    for row in principal_rows:
        name = row.get("name")
        if not name:
            continue
        principals[name] = {
            "clearance": int(row["clearance"]),
            "region": row.get("region"),
        }

    if not topics or not principals:
        raise RuntimeError("refusing to publish an empty topic or principal projection")
    return {"topics": topics, "principals": principals}


def fetch_document(driver, topic_query, principal_query):
    with driver.session() as session:
        topic_rows = [record.data() for record in session.run(topic_query)]
        principal_rows = [record.data() for record in session.run(principal_query)]
    return build_document(topic_rows, principal_rows)


def put_data(opa_data_url, document):
    body = json.dumps(document).encode("utf-8")
    request = urllib.request.Request(
        opa_data_url,
        data=body,
        method="PUT",
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(request, timeout=10) as response:
        if response.status not in (200, 204):
            raise RuntimeError(f"OPA data PUT returned {response.status}")


def policy_responds(opa_base_url):
    """True when the loaded policy returns a boolean decision."""
    payload = {
        "input": {
            "action": {
                "operation": "READ",
                "resourcePattern": {
                    "name": "credit-scores",
                    "resourceType": "TOPIC",
                    "patternType": "LITERAL",
                },
            },
            "requestContext": {"principal": {"name": "analyst", "principalType": "User"}},
        }
    }
    request = urllib.request.Request(
        opa_base_url.rstrip("/") + "/v1/data/kafka/authz/allow",
        data=json.dumps(payload).encode("utf-8"),
        method="POST",
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(request, timeout=10) as response:
        body = json.loads(response.read().decode("utf-8"))
    return isinstance(body.get("result"), bool)


def sync_once(driver, topic_query, principal_query, opa_data_url):
    document = fetch_document(driver, topic_query, principal_query)
    put_data(opa_data_url, document)
    opa_base = opa_data_url[: -len("/v1/data")] if opa_data_url.endswith("/v1/data") else opa_data_url
    if not policy_responds(opa_base):
        raise RuntimeError("OPA did not return a boolean kafka.authz.allow decision")
    return document


def main():
    from neo4j import GraphDatabase

    neo4j_uri = os.environ.get("NEO4J_URI", "bolt://neo4j:7687")
    neo4j_user = os.environ.get("NEO4J_USER", "neo4j")
    neo4j_password = os.environ.get("NEO4J_PASSWORD", "abac-local")
    opa_data_url = os.environ.get("OPA_URL", "http://opa:8181/v1/data")
    interval = int(os.environ.get("SYNC_INTERVAL_SECONDS", "15"))
    topic_query = Path(os.environ.get("TOPIC_QUERY_PATH", "/cypher/export-topics.cypher")).read_text()
    principal_query = Path(
        os.environ.get("PRINCIPAL_QUERY_PATH", "/cypher/export-principals.cypher")
    ).read_text()

    driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_password))
    try:
        while True:
            try:
                document = sync_once(driver, topic_query, principal_query, opa_data_url)
                READY_PATH.write_text("ok\n")
                print(
                    "published topics={topics} principals={principals}".format(
                        topics=sorted(document["topics"]),
                        principals=sorted(document["principals"]),
                    ),
                    flush=True,
                )
            except (OSError, urllib.error.URLError, RuntimeError, ValueError) as exc:
                print(f"sync failed: {exc}", file=sys.stderr, flush=True)
            time.sleep(interval)
    finally:
        driver.close()


if __name__ == "__main__":
    main()
