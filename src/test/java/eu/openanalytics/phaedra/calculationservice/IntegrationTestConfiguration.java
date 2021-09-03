package eu.openanalytics.phaedra.calculationservice;

import org.springframework.context.annotation.Bean;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;

public class IntegrationTestConfiguration {

    @Bean
    public Clock clock() {
        return Clock.fixed(Instant.parse("2042-12-31T23:59:59.00Z"), ZoneId.of("UTC"));
    }

}
