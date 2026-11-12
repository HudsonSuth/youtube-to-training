#!/usr/bin/env bash
# Put every jar in /usr/local/share/kafka/plugins on the broker classpath.
# A glob stored in Docker ENV is not expanded by kafka-run-class.sh.
set -euo pipefail

plugins=""
for jar in /usr/local/share/kafka/plugins/*.jar; do
  if [[ ! -f "$jar" ]]; then
    echo "OPA authorizer plugin jars are missing from /usr/local/share/kafka/plugins" >&2
    exit 1
  fi
  plugins="${plugins:+$plugins:}$jar"
done

export CLASSPATH="${plugins}${CLASSPATH:+:$CLASSPATH}"
exec /__cacert_entrypoint.sh "$@"
