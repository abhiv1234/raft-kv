# ---- build stage ----
FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /src
COPY pom.xml .
RUN mvn -q -B dependency:go-offline
COPY src ./src
RUN mvn -q -B package -DskipTests

# ---- runtime stage ----
FROM eclipse-temurin:21-jre
RUN useradd --system --uid 10001 raft && mkdir /data && chown raft /data
USER raft
COPY --from=build /src/target/raft-kv-1.0.0.jar /app/raft-kv.jar
VOLUME /data
EXPOSE 7001 8001
ENTRYPOINT ["java", "-jar", "/app/raft-kv.jar"]
