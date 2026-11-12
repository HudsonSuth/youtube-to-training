package kafka.authz

import rego.v1

default allow := false

allow if {
    not deny
}

# Example: deny reads of PII-tagged topics unless clearance is sufficient
deny if {
    is_read_operation
    topic := input.action.resourcePattern.name
    topic_meta := data.topics[topic]
    "pii" in topic_meta.tags
    principal_clearance < topic_meta.classification
}

is_read_operation if {
    input.action.operation == "READ"
}

principal_clearance := data.principals[principal_name].clearance
principal_name := input.requestContext.principal.name
