package eu.openanalytics.phaedra.calculationservice.service.protocol;

import eu.openanalytics.phaedra.calculationservice.model.Feature;
import eu.openanalytics.phaedra.scriptengine.client.model.ScriptExecution;
import eu.openanalytics.phaedra.scriptengine.dto.ScriptExecutionOutputDTO;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class FeatureCalculation {

    private final Future<Optional<ScriptExecution>> scriptExecutionFuture;
    private final Feature feature;

    private Optional<ScriptExecution> scriptExecution = Optional.empty();
    private Optional<ScriptExecutionOutputDTO> output = Optional.empty();

    public FeatureCalculation(Feature feature, Future<Optional<ScriptExecution>> scriptExecutionFuture) {
        Objects.requireNonNull(feature, "Feature cannot be null");
        Objects.requireNonNull(scriptExecutionFuture, "scriptExecutionFuture cannot be null");
        this.feature = feature;
        this.scriptExecutionFuture = scriptExecutionFuture;
    }

    public void waitForExecution() throws ExecutionException, InterruptedException {
        scriptExecution = scriptExecutionFuture.get();
    }

    public void waitForOutput() throws ExecutionException, InterruptedException {
        if (scriptExecution.isPresent()) {
            output = Optional.of(scriptExecution.get().getOutput().get());
        }
    }

    public Feature getFeature() {
        return feature;
    }

    public Optional<ScriptExecutionOutputDTO> getOutput() {
        return output;
    }
}
