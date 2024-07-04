FROM registry.openanalytics.eu/proxy/library/eclipse-temurin:21-jre-jammy

ARG JAR_FILE
ADD $JAR_FILE /opt/phaedra/service.jar

ENV USER phaedra
RUN useradd -c 'phaedra user' -m -d /home/$USER -s /bin/nologin $USER
WORKDIR /opt/phaedra
USER $USER

CMD ["java", "-jar", "/opt/phaedra/service.jar", "--spring.jmx.enabled=false"]
