# Build dependencies and compiled classes in a disposable image layer.
FROM docker.io/library/maven:3.9-eclipse-temurin-21 AS build

WORKDIR /workspace

# Copy the build descriptor first so Docker can reuse downloaded dependencies
# when only application source files change.
COPY pom.xml ./
RUN mvn -q -DskipTests dependency:go-offline

COPY src ./src
RUN mvn -q -DskipTests package dependency:copy-dependencies \
    -DincludeScope=runtime \
    -DoutputDirectory=target/dependency

# The runtime image contains only the JRE, compiled classes, and runtime jars.
FROM docker.io/library/eclipse-temurin:21-jre

WORKDIR /app
COPY --from=build /workspace/target/classes /app/classes
COPY --from=build /workspace/target/dependency /app/lib

# Compose supplies the node-specific arguments after this entry point.
ENTRYPOINT ["java", "-cp", "/app/classes:/app/lib/*", "dev.kv.cli.NodeMain"]
