package eu.openanalytics.phaedra.calculationservice.scriptengineclient.client;

import com.fasterxml.jackson.core.JsonProcessingException;
import eu.openanalytics.phaedra.calculationservice.scriptengineclient.model.ScriptExecution;

public interface ScriptEngineClient {
    ScriptExecution newScriptExecution(String targetName, String script, String input);

    void execute(ScriptExecution scriptExecution) throws JsonProcessingException;
}
