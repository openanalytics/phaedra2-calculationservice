package eu.openanalytics.phaedra.calculationservice.service.protocol;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.openanalytics.phaedra.calculationservice.model.CalculationContext;
import eu.openanalytics.phaedra.calculationservice.model.Feature;
import eu.openanalytics.phaedra.calculationservice.model.Sequence;
import eu.openanalytics.phaedra.calculationservice.service.ModelMapper;
import eu.openanalytics.phaedra.calculationservice.service.featurestat.FeatureStatExecutor;
import eu.openanalytics.phaedra.resultdataservice.client.ResultDataServiceClient;
import eu.openanalytics.phaedra.resultdataservice.dto.ResultDataDTO;
import eu.openanalytics.phaedra.resultdataservice.enumeration.StatusCode;
import eu.openanalytics.phaedra.scriptengine.client.model.ScriptExecution;
import eu.openanalytics.phaedra.scriptengine.dto.ResponseStatusCode;
import eu.openanalytics.phaedra.scriptengine.dto.ScriptExecutionOutputDTO;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

@Service
public class SequenceExecutorService {

    private final ObjectMapper objectMapper = new ObjectMapper(); // TODO thread-safe?

    private final ResultDataServiceClient resultDataServiceClient;
    private final FeatureExecutorService featureExecutorService;
    private final ModelMapper modelMapper;
    private final FeatureStatExecutor featureStatExecutor; // TODO remove deps?

    public SequenceExecutorService(ResultDataServiceClient resultDataServiceClient, FeatureExecutorService featureExecutorService, ModelMapper modelMapper, FeatureStatExecutor featureStatExecutor) {
        this.resultDataServiceClient = resultDataServiceClient;
        this.featureExecutorService = featureExecutorService;
        this.modelMapper = modelMapper;
        this.featureStatExecutor = featureStatExecutor;
    }


    public boolean executeSequence(CalculationContext cctx, ExecutorService executorService, Sequence currentSequence) {
        // A. asynchronously create inputs and submit them to the ScriptEngine
        var calculations = new ArrayList<FeatureCalculation>();

        for (var feature : currentSequence.getFeatures()) {
            calculations.add(new FeatureCalculation(feature, executorService.submit(() ->
                    featureExecutorService.executeFeature(cctx, feature, currentSequence.getSequenceNumber()))));
        }

        // B. wait (block !) for execution to be sent to the ScriptEngine
        for (var calculation : calculations) {
            try {
                calculation.waitForExecution();
            } catch (InterruptedException e) {
                cctx.errorCollector().handleError("executing sequence => waiting for feature to be sent => interrupted", e, calculation.getFeature(), calculation.getFeature().getFormula());
            } catch (ExecutionException e) {
                cctx.errorCollector().handleError("executing sequence => waiting for feature to be sent => exception during execution", e.getCause(), calculation.getFeature(), calculation.getFeature().getFormula());
            } catch (Throwable e) {
                cctx.errorCollector().handleError("executing sequence => waiting for feature to be sent => exception during execution", e, calculation.getFeature(), calculation.getFeature().getFormula());
            }
        }

        // C. wait (block !) for output to be received from the ScriptEngine
        for (var calculation : calculations) {
            try {
                calculation.waitForOutput();
            } catch (InterruptedException e) {
                cctx.errorCollector().handleError("executing sequence => waiting for output to be received => interrupted", e, calculation.getFeature(), calculation.getFeature().getFormula());
            } catch (ExecutionException e) {
                cctx.errorCollector().handleError("executing sequence => waiting for output to be received => exception during execution", e.getCause(), calculation.getFeature(), calculation.getFeature().getFormula());
            } catch (Throwable e) {
                cctx.errorCollector().handleError("executing sequence => waiting for output to be received => exception during execution", e, calculation.getFeature(), calculation.getFeature().getFormula());
            }
        }

        // D. save the output
        for (var calculation : calculations) {
            var resultData = saveOutput(cctx, calculation);
            if (resultData.isPresent() && resultData.get().getStatusCode() == StatusCode.SUCCESS) {
                // E. trigger calculation of FeatureStats for the features in this Sequence
                cctx.computedStatsForFeature().put(calculation.getFeature(), executorService.submit(() -> featureStatExecutor.executeFeatureStat(cctx, calculation.getFeature(), resultData.get())));
            }
        }

        return !cctx.errorCollector().hasError();
    }

    public Optional<ResultDataDTO> saveOutput(CalculationContext cctx, FeatureCalculation calculation) {
        if (calculation.getOutput().isEmpty()) {
            return Optional.empty();
        }
        var output = calculation.getOutput().get();
        var feature = calculation.getFeature();
        try {
            if (output.getStatusCode() == ResponseStatusCode.SUCCESS) {
                try {
                    OutputWrapper outputValue = objectMapper.readValue(output.getOutput(), OutputWrapper.class);
                    var resultData = resultDataServiceClient.addResultData(
                            cctx.resultSetId(),
                            feature.getId(),
                            outputValue.output,
                            modelMapper.map(output.getStatusCode()),
                            output.getStatusMessage(),
                            output.getExitCode());

                    return Optional.of(resultData);
                } catch (JsonProcessingException e) {
                    cctx.errorCollector().handleError( "executing sequence => processing output => parsing output", e, feature, output, feature.getFormula());
                }
            } else if (output.getStatusCode() == ResponseStatusCode.SCRIPT_ERROR) {
                var resultData = resultDataServiceClient.addResultData(
                        cctx.resultSetId(),
                        feature.getId(),
                        new float[]{},
                        modelMapper.map(output.getStatusCode()),
                        output.getStatusMessage(),
                        output.getExitCode());

                cctx.errorCollector().handleError("executing sequence => processing output => output indicates script error", output, feature, feature.getFormula());
                return Optional.of(resultData);
            } else if (output.getStatusCode() == ResponseStatusCode.WORKER_INTERNAL_ERROR) {
                // TODO re-schedule script?
            }
        } catch (Exception e) {
            cctx.errorCollector().handleError("executing sequence => processing output => saving resultdata", e, feature, feature.getFormula());
        }
        return Optional.empty();
    }

    private static class OutputWrapper {

        public final float[] output;

        @JsonCreator
        private OutputWrapper(@JsonProperty(value = "output", required = true) float[] output) {
            this.output = output;
        }
    }

    private static class FeatureCalculation {

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
}
