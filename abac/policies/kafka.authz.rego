package kafka.authz

import rego.v1

default allow := false

# WRITE and other non-topic operations stay allowed so producers work.
# Topic reads are not covered here, so a missing deny cannot turn into an allow.
allow if {
    not is_topic_read
}

# A topic read is allowed only when the topic, the principal, and a numeric
# clearance are all present, and the PII rule does not deny it.
allow if {
    is_topic_read
    topic_meta := data.topics[topic_name]
    principal := data.principals[principal_name]
    clearance := principal.clearance
    is_number(clearance)
    not pii_read_denied(topic_meta, clearance)
}

pii_read_denied(topic_meta, clearance) if {
    "pii" in topic_meta.tags
    clearance < topic_meta.classification
}

is_topic_read if {
    input.action.operation == "READ"
    input.action.resourcePattern.resourceType == "TOPIC"
}

topic_name := input.action.resourcePattern.name
principal_name := input.requestContext.principal.name
