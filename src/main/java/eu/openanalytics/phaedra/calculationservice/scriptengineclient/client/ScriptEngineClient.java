package eu.openanalytics.phaedra.calculationservice.scriptengineclient.client;

import com.fasterxml.jackson.core.JsonProcessingException;
import eu.openanalytics.phaedra.calculationservice.scriptengineclient.model.ScriptExecutionInput;

public interface ScriptEngineClient {
    ScriptExecutionInput newScriptExecution(String targetName, String script, String input);

    void execute(ScriptExecutionInput input) throws JsonProcessingException;
}
