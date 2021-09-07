package eu.openanalytics.phaedra.calculationservice;

import eu.openanalytics.phaedra.scriptengine.client.ScriptEngineClient;
import eu.openanalytics.phaedra.scriptengine.client.model.ScriptExecution;
import org.springframework.context.annotation.Bean;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;

public class IntegrationTestConfiguration {

    @Bean
    public Clock clock() {
        return Clock.fixed(Instant.parse("2042-12-31T23:59:59.00Z"), ZoneId.of("UTC"));
    }

    @Bean
    public ScriptEngineClient scriptEngineClient() {
        return new ScriptEngineClient() {
            @Override
            public ScriptExecution newScriptExecution(String targetName, String script, String input) {
                return null;
            }

            @Override
            public void execute(ScriptExecution scriptExecution) {
            }
        };
    }

}
