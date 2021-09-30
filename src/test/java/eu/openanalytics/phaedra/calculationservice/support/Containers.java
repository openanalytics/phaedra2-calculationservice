package eu.openanalytics.phaedra.calculationservice.support;

import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;

public class Containers {

    @Container
    public static final PostgreSQLContainer<?> postgreSQLContainer;

    static {
        postgreSQLContainer = new PostgreSQLContainer<>("postgres:13-alpine");
        postgreSQLContainer.start();
    }
}

