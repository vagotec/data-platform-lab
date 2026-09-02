FROM eclipse-temurin:25-jre

WORKDIR /app

COPY target/customer-streams-1.3.0.jar /app/customer-streams.jar

USER 1000

ENTRYPOINT ["java", "--enable-native-access=ALL-UNNAMED", "-jar", "/app/customer-streams.jar"]
