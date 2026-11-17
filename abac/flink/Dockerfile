FROM maven:3.9.9-eclipse-temurin-17 AS build
WORKDIR /src
COPY pom.xml .
COPY src ./src
RUN mvn -q -DskipTests package

FROM flink:1.18.1-java17
COPY --from=build /src/target/field-mask.jar /opt/job/field-mask.jar
COPY run.sh /opt/job/run.sh
RUN chmod 755 /opt/job/run.sh
ENTRYPOINT ["/opt/job/run.sh"]
