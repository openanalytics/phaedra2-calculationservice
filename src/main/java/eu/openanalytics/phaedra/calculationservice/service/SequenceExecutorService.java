package eu.openanalytics.phaedra.calculationservice.service;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.openanalytics.phaedra.calculationservice.controller.clients.ResultDataServiceClient;
import eu.openanalytics.phaedra.calculationservice.dto.external.ResultSetDTO;
import eu.openanalytics.phaedra.calculationservice.model.Feature;
import eu.openanalytics.phaedra.calculationservice.model.Sequence;
import eu.openanalytics.phaedra.calculationservice.scriptengineclient.model.ResponseStatusCode;
import eu.openanalytics.phaedra.calculationservice.scriptengineclient.model.ScriptExecutionInput;
import eu.openanalytics.phaedra.calculationservice.scriptengineclient.model.ScriptExecutionOutput;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

@Service
public class SequenceExecutorService {

    private final ObjectMapper objectMapper = new ObjectMapper(); // TODO thread-safe?

    private final ResultDataServiceClient resultDataServiceClient;
    private final FeatureExecutorService featureExecutorService;

    public SequenceExecutorService(ResultDataServiceClient resultDataServiceClient, FeatureExecutorService featureExecutorService) {
        this.resultDataServiceClient = resultDataServiceClient;
        this.featureExecutorService = featureExecutorService;
    }

    public boolean executeSequence(ExecutorService executorService, ErrorCollector errorCollector, Sequence currentSequence, long measId, ResultSetDTO resultSet) {
        // A. asynchronously create inputs and submit them to the ScriptEngine
        var calculations = new ArrayList<Pair<Feature, Future<Optional<ScriptExecutionInput>>>>();

        for (var feature : currentSequence.getFeatures()) {
            calculations.add(Pair.of(feature, executorService.submit(() ->
                    featureExecutorService.executeFeature(errorCollector, feature, measId, currentSequence.getSequenceNumber(), resultSet.getId()))));
        }

        // B. wait (block !) for execution to be sent to the ScriptEngine
        var outputFutures = new ArrayList<Pair<Feature, Future<ScriptExecutionOutput>>>();
        for (var calculation : calculations) {
            try {
                if (calculation.getRight().get().isPresent()) {
                    outputFutures.add(Pair.of(calculation.getLeft(), calculation.getRight().get().get().getOutput()));
                }
            } catch (InterruptedException e) {
                errorCollector.handleError(e, "executing sequence => waiting for feature to be sent => interrupted", calculation.getLeft());
            } catch (ExecutionException e) {
                errorCollector.handleError(e.getCause(), "executing sequence => waiting for feature to be sent => exception during execution", calculation.getLeft());
            } catch (Throwable e) {
                errorCollector.handleError(e, "executing sequence => waiting for feature to be sent => exception during execution", calculation.getLeft());
            }
        }

        // C. wait (block !) for output to be received from the ScriptEngine
        var outputs = new ArrayList<Pair<Feature, ScriptExecutionOutput>>();
        for (var outputFuture : outputFutures) {
            try {
                outputs.add(Pair.of(outputFuture.getLeft(), outputFuture.getRight().get()));
            } catch (InterruptedException e) {
                errorCollector.handleError(e, "executing sequence => waiting for output to be received => interrupted", outputFuture.getLeft());
            } catch (ExecutionException e) {
                errorCollector.handleError(e.getCause(), "executing sequence => waiting for output to be received => exception during execution", outputFuture.getLeft());
            } catch (Throwable e) {
                errorCollector.handleError(e, "executing sequence => waiting for output to be received => exception during execution", outputFuture.getLeft());
            }
        }

        // D. save the output
        for (var el : outputs) {
            saveOutput(errorCollector, resultSet, el.getLeft(), el.getRight());
        }

        return !errorCollector.hasError();
    }

    public void saveOutput(ErrorCollector errorCollector, ResultSetDTO resultSet, Feature feature, ScriptExecutionOutput output) {
        try {
            if (output.getStatusCode() == ResponseStatusCode.SUCCESS) {
                try {
                    OutputWrapper outputValue = objectMapper.readValue(output.getOutput(), OutputWrapper.class);
                    resultDataServiceClient.addResultData(
                            resultSet.getId(),
                            feature.getId(),
                            outputValue.output,
                            output.getStatusCode(),
                            output.getStatusMessage(),
                            output.getExitCode());
                } catch (JsonProcessingException e) {
                    errorCollector.handleError(e, "executing sequence => processing output => parsing output", feature, output);
                }
            } else if (output.getStatusCode() == ResponseStatusCode.SCRIPT_ERROR) {
                resultDataServiceClient.addResultData(
                        resultSet.getId(),
                        feature.getId(),
                        new float[]{},
                        output.getStatusCode(),
                        output.getStatusMessage(),
                        output.getExitCode());

                errorCollector.handleScriptError(output, feature);
            } else if (output.getStatusCode() == ResponseStatusCode.WORKER_INTERNAL_ERROR) {
                // TODO re-schedule script?
            }
        } catch (Exception e) {
            errorCollector.handleError(e, "executing sequence => processing output => saving resultdata", feature);
        }
    }

    static class OutputWrapper {

        public final float[] output;

        @JsonCreator
        private OutputWrapper(@JsonProperty(value = "output", required = true) float[] output) {
            this.output = output;
        }
    }

}
