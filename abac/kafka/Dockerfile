FROM apache/kafka:3.8.1

# The published 1.5.1 jar does not embed Guava. Kafka 3.8.1 already ships
# scala-logging and jackson-module-scala, which the plugin also needs.
USER root
RUN mkdir -p /usr/local/share/kafka/plugins
ADD https://repo1.maven.org/maven2/org/openpolicyagent/kafka/opa-authorizer/1.5.1/opa-authorizer-1.5.1.jar /usr/local/share/kafka/plugins/opa-authorizer-1.5.1.jar
ADD https://repo1.maven.org/maven2/com/google/guava/guava/30.1-jre/guava-30.1-jre.jar /usr/local/share/kafka/plugins/guava-30.1-jre.jar
ADD https://repo1.maven.org/maven2/com/google/guava/failureaccess/1.0.1/failureaccess-1.0.1.jar /usr/local/share/kafka/plugins/failureaccess-1.0.1.jar
COPY docker-entrypoint.sh /usr/local/bin/kafka-abac-entrypoint.sh
RUN chmod 755 /usr/local/bin/kafka-abac-entrypoint.sh \
    && chown -R appuser:appuser /usr/local/share/kafka/plugins
USER appuser

ENTRYPOINT ["/usr/local/bin/kafka-abac-entrypoint.sh"]
CMD ["/etc/kafka/docker/run"]
