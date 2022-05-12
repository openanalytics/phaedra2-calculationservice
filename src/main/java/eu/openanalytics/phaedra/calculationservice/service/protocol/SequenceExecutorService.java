/**
 * Phaedra II
 *
 * Copyright (C) 2016-2022 Open Analytics
 *
 * ===========================================================================
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the Apache License as published by
 * The Apache Software Foundation, either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Apache License for more details.
 *
 * You should have received a copy of the Apache License
 * along with this program.  If not, see <http://www.apache.org/licenses/>
 */
package eu.openanalytics.phaedra.calculationservice.service.protocol;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.primitives.Doubles;
import com.google.common.primitives.Floats;
import eu.openanalytics.phaedra.calculationservice.model.CalculationContext;
import eu.openanalytics.phaedra.calculationservice.model.Feature;
import eu.openanalytics.phaedra.calculationservice.model.Sequence;
import eu.openanalytics.phaedra.calculationservice.model.SuccessTracker;
import eu.openanalytics.phaedra.calculationservice.service.ModelMapper;
import eu.openanalytics.phaedra.calculationservice.service.featurestat.FeatureStatExecutor;
import eu.openanalytics.phaedra.resultdataservice.client.ResultDataServiceClient;
import eu.openanalytics.phaedra.resultdataservice.dto.ResultDataDTO;
import eu.openanalytics.phaedra.resultdataservice.enumeration.StatusCode;
import eu.openanalytics.phaedra.scriptengine.client.model.ScriptExecution;
import eu.openanalytics.phaedra.scriptengine.dto.ResponseStatusCode;
import eu.openanalytics.phaedra.scriptengine.dto.ScriptExecutionOutputDTO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import static eu.openanalytics.phaedra.calculationservice.service.protocol.ProtocolLogger.log;

@Service
public class SequenceExecutorService {

    private final ObjectMapper objectMapper = new ObjectMapper(); // TODO thread-safe?
    private final ResultDataServiceClient resultDataServiceClient;
    private final FeatureExecutorService featureExecutorService;
    private final ModelMapper modelMapper;
    private final FeatureStatExecutor featureStatExecutor; // TODO remove deps?
    private final Logger logger = LoggerFactory.getLogger(getClass());

    private final static int MAX_ATTEMPTS = 3;

    public SequenceExecutorService(ResultDataServiceClient resultDataServiceClient, FeatureExecutorService featureExecutorService, ModelMapper modelMapper, FeatureStatExecutor featureStatExecutor) {
        this.resultDataServiceClient = resultDataServiceClient;
        this.featureExecutorService = featureExecutorService;
        this.modelMapper = modelMapper;
        this.featureStatExecutor = featureStatExecutor;
    }

    public boolean executeSequence(CalculationContext cctx, ExecutorService executorService, Sequence currentSequence, String... authToken) {
        var sequenceSuccess = new SuccessTracker<Void>();
        log(logger, cctx, "[S=%s] Executing sequence", currentSequence.getSequenceNumber());

        // 1. try to calculate all features
        var featuresToCalculate = currentSequence.getFeatures();
        final var featureCount = featuresToCalculate.size(); // total number of features to calculate
        var calculations = new ArrayList<FeatureCalculation>();
        for (int attempt = 1; true; attempt++) {
            log(logger, cctx, "[S=%s] Attempt %s to calculate [%s of %s] features", currentSequence.getSequenceNumber(), attempt, featuresToCalculate.size(), featureCount);

            var currentAttempt = calculateFeatures(cctx, executorService, currentSequence, featuresToCalculate, authToken);
            sequenceSuccess.failedIfStepFailed(currentAttempt); // sequence is failed if the current attempt failed

            if (!currentAttempt.isSuccess() || attempt == MAX_ATTEMPTS) {
                // 2. there was a hard-error or we are at the last attempt -> do not re-attempt any calculation
                calculations.addAll(currentAttempt.getResult());
                break;
            }

            // 3. check if we have to retry any of the calculations
            var featuresToRetry = new ArrayList<Feature>();
            for (var calculation : currentAttempt.getResult()) {
                if (calculation.getOutput().isPresent() && calculation.getOutput().get().getStatusCode().canBeRetried()) {
                    featuresToRetry.add(calculation.getFeature());
                } else {
                    calculations.add(calculation);
                }
            }
            if (featuresToRetry.size() == 0) {
                // 4. nothing to retry
                break;
            }
            featuresToCalculate = featuresToRetry;
        }

        // 5. save the output
       for (var calculation : calculations) {
        	try {
        		var resultData = saveOutput(cctx, calculation, authToken);
        		if (resultData.isPresent() && resultData.get().getStatusCode() == StatusCode.SUCCESS) {
        			// E. trigger calculation of FeatureStats for the features in this Sequence
        			cctx.getComputedStatsForFeature().put(calculation.getFeature(), executorService.submit(() -> featureStatExecutor.executeFeatureStat(cctx, calculation.getFeature(), resultData.get(), authToken)));
        		} else {
        			sequenceSuccess.failed();
        		}
            } catch (Throwable e) {
                cctx.getErrorCollector().handleError("executing sequence => saving output", e, calculation.getFeature(), calculation.getFeature().getFormula());
            }
        }

        log(logger, cctx, "[S=%s] Finished: success: %s", currentSequence.getSequenceNumber(), sequenceSuccess.isSuccess());
        return sequenceSuccess.isSuccess();
    }

    public SuccessTracker<ArrayList<FeatureCalculation>> calculateFeatures(CalculationContext cctx, ExecutorService executorService, Sequence currentSequence, List<Feature> features, String... authToken) {
        // A. asynchronously create inputs and submit them to the ScriptEngine
        var calculations = new ArrayList<FeatureCalculation>();
        var success = new SuccessTracker<ArrayList<FeatureCalculation>>();

        for (var feature : features) {
            calculations.add(new FeatureCalculation(feature, executorService.submit(() ->
                    featureExecutorService.executeFeature(cctx, feature, currentSequence.getSequenceNumber(), authToken))));
        }

        // B. wait (block !) for execution to be sent to the ScriptEngine
        for (var calculation : calculations) {
            try {
                calculation.waitForExecution();
            } catch (InterruptedException e) {
                cctx.getErrorCollector().handleError("executing sequence => waiting for feature to be sent => interrupted", e, calculation.getFeature(), calculation.getFeature().getFormula());
                success.failed();
            } catch (ExecutionException e) {
                cctx.getErrorCollector().handleError("executing sequence => waiting for feature to be sent => exception during execution", e.getCause(), calculation.getFeature(), calculation.getFeature().getFormula());
                success.failed();
            } catch (Throwable e) {
                cctx.getErrorCollector().handleError("executing sequence => waiting for feature to be sent => exception during execution", e, calculation.getFeature(), calculation.getFeature().getFormula());
                success.failed();
            }
        }

        log(logger, cctx, "[S=%s] All calculations send to script engine", currentSequence.getSequenceNumber());

        // C. wait (block !) for output to be received from the ScriptEngine
        for (var calculation : calculations) {
            try {
                calculation.waitForOutput();
            } catch (InterruptedException e) {
                cctx.getErrorCollector().handleError("executing sequence => waiting for output to be received => interrupted", e, calculation.getFeature(), calculation.getFeature().getFormula());
                success.failed();
            } catch (ExecutionException e) {
                cctx.getErrorCollector().handleError("executing sequence => waiting for output to be received => exception during execution", e.getCause(), calculation.getFeature(), calculation.getFeature().getFormula());
                success.failed();
            } catch (Throwable e) {
                cctx.getErrorCollector().handleError("executing sequence => waiting for output to be received => exception during execution", e, calculation.getFeature(), calculation.getFeature().getFormula());
                success.failed();
            }
        }

        log(logger, cctx, "[S=%s] All outputs received from script engine", currentSequence.getSequenceNumber());
        success.setResult(calculations);
        return success;
    }

    public Optional<ResultDataDTO> saveOutput(CalculationContext cctx, FeatureCalculation calculation, String... authToken) {
        if (calculation.getOutput().isEmpty()) {
            return Optional.empty();
        }
        var output = calculation.getOutput().get();
        var feature = calculation.getFeature();
        try {
            if (output.getStatusCode() == ResponseStatusCode.SUCCESS) {
                try {
                    OutputWrapper outputValue = objectMapper.readValue(output.getOutput(), OutputWrapper.class);
                    float[] floatOutputValue = new float[outputValue.output.length];
                    for (int i = 0; i < outputValue.output.length; i++) {
                        try {
                            floatOutputValue[i] = Float.parseFloat(outputValue.output[i]);
                        } catch (Exception e) {
                            floatOutputValue[i] = Float.NaN;
                        }
                    }

                    var resultData = resultDataServiceClient.addResultData(
                            cctx.getResultSetId(),
                            feature.getId(),
                            floatOutputValue,
                            modelMapper.map(output.getStatusCode()),
                            output.getStatusMessage(),
                            output.getExitCode(),
                            authToken);

                    return Optional.of(resultData);
                } catch (JsonProcessingException e) {
                    cctx.getErrorCollector().handleError("executing sequence => processing output => parsing output", e, feature, output, feature.getFormula());
                }
            } else {
                var resultData = resultDataServiceClient.addResultData(
                        cctx.getResultSetId(),
                        feature.getId(),
                        new float[]{},
                        modelMapper.map(output.getStatusCode()),
                        output.getStatusMessage(),
                        output.getExitCode(),
                        authToken);

                cctx.getErrorCollector().handleError(String.format("executing sequence => processing output => output indicates error [%s]", output.getStatusCode()), output, feature, feature.getFormula());
                return Optional.of(resultData);
            }
        } catch (Exception e) {
            cctx.getErrorCollector().handleError("executing sequence => processing output => saving resultdata", e, feature, feature.getFormula());
        }
        return Optional.empty();
    }

    private static class OutputWrapper {

        public final String[] output;

        @JsonCreator
        private OutputWrapper(@JsonProperty(value = "output", required = true) String[] output) {
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
