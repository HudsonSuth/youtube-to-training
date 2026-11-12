#!/usr/bin/env bash
set -euo pipefail

classpath="/opt/job/field-mask.jar"
for jar in /opt/flink/lib/*.jar; do
  classpath="${classpath}:${jar}"
done

exec java -cp "$classpath" abac.mask.FieldMaskJob
