FROM openjdk:16-jdk-alpine
ADD target/phaedra2-calculationservice-0.0.1-SNAPSHOT.jar app.jar
ENV JAVA_OPTS=""
ENTRYPOINT [ "sh", "-c", "java $JAVA_OPTS -jar /app.jar" ]
