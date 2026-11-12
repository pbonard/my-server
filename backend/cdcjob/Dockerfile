FROM eclipse-temurin:21-jdk AS builder

WORKDIR /app
COPY gradlew settings.gradle build.gradle ./
COPY gradle/ gradle/
RUN chmod +x gradlew
RUN ./gradlew dependencies --no-daemon || true

COPY src/ src/
RUN ./gradlew shadowJar --no-daemon

FROM flink:2.2.1-java21

COPY --from=builder /app/build/libs/cdcjob-1.0-SNAPSHOT.jar /opt/flink/usrlib/cdcjob.jar
