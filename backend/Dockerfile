FROM amazoncorretto:21-alpine AS build
WORKDIR /app
COPY gradlew settings.gradle.kts build.gradle.kts gradle.properties ./
COPY gradle ./gradle
RUN chmod +x gradlew && ./gradlew dependencies --no-daemon || true
COPY src ./src
RUN ./gradlew bootJar --no-daemon -x test

FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=build /app/build/libs/*.jar app.jar
RUN mkdir -p /data
ENV DFVIEWER_CACHE_DIR=/data
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "app.jar"]
