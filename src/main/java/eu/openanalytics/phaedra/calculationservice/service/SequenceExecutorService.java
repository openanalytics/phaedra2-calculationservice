package eu.openanalytics.phaedra.calculationservice.service;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.openanalytics.phaedra.calculationservice.controller.clients.ResultDataServiceClient;
import eu.openanalytics.phaedra.calculationservice.controller.clients.ResultDataUnresolvableException;
import eu.openanalytics.phaedra.calculationservice.dto.external.ResultSetDTO;
import eu.openanalytics.phaedra.calculationservice.model.Feature;
import eu.openanalytics.phaedra.calculationservice.model.Sequence;
import eu.openanalytics.phaedra.calculationservice.scriptengineclient.model.ResponseStatusCode;
import eu.openanalytics.phaedra.calculationservice.scriptengineclient.model.ScriptExecutionInput;
import eu.openanalytics.phaedra.calculationservice.scriptengineclient.model.ScriptExecutionOutput;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
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
        var calculations = new ArrayList<Pair<Feature, Future<ScriptExecutionInput>>>();

        for (var feature : currentSequence.getFeatures()) {
            calculations.add(Pair.of(feature, executorService.submit(() ->
                    featureExecutorService.executeFeature(errorCollector, feature, measId, currentSequence.getSequenceNumber(), resultSet.getId()))));
        }

        // B. wait (block !) for execution to be sent to the ScriptEngine
        var outputFutures = safelyMapFutures(errorCollector, calculations,
                (calculation) -> {
                    var res = calculation.get();
                    if (res != null) {
                        return (Future<ScriptExecutionOutput>) res.getOutput();
                    }
                    return null;
                },
                "executing sequence => waiting for feature to be sent"
        );

        // C. wait (block !) for outputs to be received from ScriptEngine
        var outputs = safelyMapFutures(errorCollector, outputFutures,
                Future::get,
                "executing sequence => waiting for output to be received");

        // D. check for errors
        if (outputs == null || errorCollector.hasError()) {
            // -> we got an error, do not process the output
            return false;
        }

        // G. process the output
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
                    errorCollector.handleError(e, "executing sequence => processing output => parsing output", feature);
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

    public interface Map<T, R> {
        R apply(T var1) throws InterruptedException, ExecutionException;
    }

    public <T, R> ArrayList<Pair<Feature, R>> safelyMapFutures(ErrorCollector errorCollector,
                                                               List<Pair<Feature, Future<T>>> futures,
                                                               Map<Future<T>, R> map,
                                                               String location) {

        // pre-execution: check for errors
        if (errorCollector.hasError()) {
            // -> we got an error, cancel not started tasks, but don't interrupt them
            if (futures == null) return null;
            for (var el : futures) {
                if (el != null && el.getRight() != null) {
                    el.getRight().cancel(false);
                }
            }
            return null;
        }

        var res = new ArrayList<Pair<Feature, R>>();

        for (var el : futures) {
            try {
                res.add(Pair.of(el.getLeft(), map.apply(el.getRight())));
            } catch (InterruptedException e) {
                errorCollector.handleError(e, location + " => interrupted", el.getLeft());
            } catch (ExecutionException e) {
                errorCollector.handleError(e.getCause(), location + " => exception during execution", el.getLeft());
            } catch (Exception e) {
                errorCollector.handleError(e, location + " => exception during execution", el.getLeft());
            }
        }

        return res;
    }

    static class OutputWrapper {

        public final float[] output;

        @JsonCreator
        private OutputWrapper(@JsonProperty(value = "output", required = true) float[] output) {
            this.output = output;
        }
    }

}
