package eu.openanalytics.phaedra.calculationservice.scriptengineclient.model;

import eu.openanalytics.phaedra.model.v2.dto.ScriptExecutionOutputDTO;
import eu.openanalytics.phaedra.model.v2.dto.ScriptExecutionInputDTO;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

public class ScriptExecution {

    private final CompletableFuture<ScriptExecutionOutputDTO> output = new CompletableFuture<>();
    private final TargetRuntime targetRuntime;
    private final ScriptExecutionInputDTO scriptExecutionInput;

    public ScriptExecution(
            TargetRuntime targetRuntime,
            String script,
            String input,
            String responseTopicSuffix) {
        this.targetRuntime = targetRuntime;
        this.scriptExecutionInput = ScriptExecutionInputDTO.builder()
                .id(UUID.randomUUID().toString())
                .script(script)
                .input(input)
                .responseTopicSuffix(responseTopicSuffix)
                .build();
    }

    public ScriptExecutionInputDTO getScriptExecutionInput() {
        return scriptExecutionInput;
    }

    public TargetRuntime getTargetRuntime() {
        return targetRuntime;
    }

    public CompletableFuture<ScriptExecutionOutputDTO> getOutput() {
        return output;
    }

}
