package eu.openanalytics.phaedra.calculationservice.scriptengineclient.model;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

// TODO get rid of jackson here

public class ScriptExecutionInput {

    private final UUID id;
    private final String script;
    private final String input;
    private final String responseTopicSuffix;
    private final CompletableFuture<ScriptExecutionOutput> output = new CompletableFuture<>();
    private final TargetRuntime targetRuntime;

    public ScriptExecutionInput(
            TargetRuntime targetRuntime,
            String script,
            String input,
            String responseTopicSuffix) {
        this.targetRuntime = targetRuntime;
        this.id = UUID.randomUUID();
        this.script = script;
        this.input = input;
        this.responseTopicSuffix = responseTopicSuffix;
    }

    public String getInput() {
        return input;
    }

    public String getScript() {
        return script;
    }

    public String getResponseTopicSuffix() {
        return responseTopicSuffix;
    }

    public UUID getId() {
        return id;
    }

    public CompletableFuture<ScriptExecutionOutput> getOutput() {
        return output;
    }

    public TargetRuntime getTargetRuntime() {
        return targetRuntime;
    }
}
