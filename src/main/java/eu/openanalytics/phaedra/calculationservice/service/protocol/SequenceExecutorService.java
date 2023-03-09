/**
 * Phaedra II
 *
 * Copyright (C) 2016-2023 Open Analytics
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

import static eu.openanalytics.phaedra.calculationservice.util.LoggerHelper.log;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import eu.openanalytics.phaedra.calculationservice.dto.CurveFittingRequestDTO;
import eu.openanalytics.phaedra.calculationservice.model.CalculationContext;
import eu.openanalytics.phaedra.calculationservice.model.Formula;
import eu.openanalytics.phaedra.calculationservice.model.ModelMapper;
import eu.openanalytics.phaedra.calculationservice.service.KafkaProducerService;
import eu.openanalytics.phaedra.calculationservice.util.SuccessTracker;
import eu.openanalytics.phaedra.protocolservice.dto.FeatureDTO;
import eu.openanalytics.phaedra.resultdataservice.dto.ResultDataDTO;
import eu.openanalytics.phaedra.resultdataservice.enumeration.StatusCode;
import eu.openanalytics.phaedra.scriptengine.client.model.ScriptExecution;
import eu.openanalytics.phaedra.scriptengine.dto.ResponseStatusCode;
import eu.openanalytics.phaedra.scriptengine.dto.ScriptExecutionOutputDTO;

/**
 * A sequence is a group of features that may depend on features from the
 * previous sequence, but do not depend on features from the same sequence
 * or any later sequence.
 * 
 * In other words, all features from a sequence can be calculated in parallel.
 */
@Service
public class SequenceExecutorService {

    private final FeatureExecutorService featureExecutorService;
    private final FeatureStatExecutorService featureStatExecutorService;
    
    private final KafkaProducerService kafkaProducerService;
    private final ObjectMapper objectMapper;
    private final ModelMapper modelMapper;
    private final Logger logger = LoggerFactory.getLogger(getClass());

    private final static int MAX_ATTEMPTS = 3;

    public SequenceExecutorService(FeatureExecutorService featureExecutorService, ModelMapper modelMapper, ObjectMapper objectMapper,
                                   FeatureStatExecutorService featureStatExecutorService, KafkaProducerService kafkaProducerService) {
        this.featureExecutorService = featureExecutorService;
        this.modelMapper = modelMapper;
        this.objectMapper = objectMapper;
        this.featureStatExecutorService = featureStatExecutorService;
        this.kafkaProducerService = kafkaProducerService;
    }

    public boolean executeSequence(CalculationContext ctx, ExecutorService executorService, Integer currentSequence) {
        var sequenceSuccess = new SuccessTracker<Void>();
        log(logger, ctx, "[S=%d] Executing sequence", currentSequence);

        var featuresToCalculate = ctx.getProtocolData().protocol.getFeatures().stream()
        		.filter(f -> f.getSequence() == currentSequence).toList();
        
        var calculations = new ArrayList<FeatureCalculation>();
        
        for (int attempt = 1; true; attempt++) {
            log(logger, ctx, "[S=%s] Attempt %s to calculate [%s of %s] features", currentSequence, attempt, featuresToCalculate.size(), featuresToCalculate.size());

            var currentAttempt = calculateFeatures(ctx, executorService, currentSequence, featuresToCalculate);
            sequenceSuccess.failedIfStepFailed(currentAttempt); // sequence is failed if the current attempt failed

            if (!currentAttempt.isSuccess() || attempt == MAX_ATTEMPTS) {
                // There was a hard-error or we are at the last attempt -> do not re-attempt any calculation
                calculations.addAll(currentAttempt.getResult());
                break;
            }

            // Check if we have to retry any of the calculations
            var featuresToRetry = new ArrayList<FeatureDTO>();
            for (var calculation : currentAttempt.getResult()) {
                if (calculation.getOutput().isPresent() && calculation.getOutput().get().getStatusCode().canBeRetried()) {
                    featuresToRetry.add(calculation.getFeature());
                } else {
                    calculations.add(calculation);
                }
            }
            if (featuresToRetry.size() == 0) {
                break;
            }
            featuresToCalculate = featuresToRetry;
        }

        // Save the output
       for (var calculation : calculations) {
        	try {
        		var resultData = emitFeatureCalculationOutput(ctx, calculation);
        		if (resultData.isPresent() && resultData.get().getStatusCode() == StatusCode.SUCCESS) {
        			// Trigger calculation of FeatureStats for the features in this Sequence
        			ctx.getComputedStatsForFeature().put(calculation.getFeature(), executorService.submit(() -> featureStatExecutorService.executeFeatureStat(ctx, calculation.getFeature(), resultData.get())));
        		} else {
        			sequenceSuccess.failed();
        		}
            } catch (Throwable e) {
                ctx.getErrorCollector().addError("executing sequence => saving output", e, 
                		calculation.getFeature(), ctx.getProtocolData().formulas.get(calculation.getFeature().getFormulaId()));
            }
        }

        log(logger, ctx, "[S=%s] Finished: success: %s", currentSequence, sequenceSuccess.isSuccess());
        return sequenceSuccess.isSuccess();
    }

    public SuccessTracker<ArrayList<FeatureCalculation>> calculateFeatures(CalculationContext ctx, ExecutorService executorService, Integer currentSequence, List<FeatureDTO> features) {
        // A. asynchronously create inputs and submit them to the ScriptEngine
        var calculations = new ArrayList<FeatureCalculation>();
        var success = new SuccessTracker<ArrayList<FeatureCalculation>>();
        for (var feature : features) {
            calculations.add(new FeatureCalculation(feature, executorService.submit(() ->
                    featureExecutorService.executeFeature(ctx, feature, currentSequence))));
        }

        // B. wait (block !) for execution to be sent to the ScriptEngine
        for (var calculation : calculations) {
        	Formula formula = ctx.getProtocolData().formulas.get(calculation.getFeature().getFormulaId());
            try {
                calculation.waitForExecution();
            } catch (InterruptedException e) {
                ctx.getErrorCollector().addError("executing sequence => waiting for feature to be sent => interrupted", e, calculation.getFeature(), formula);
                success.failed();
            } catch (ExecutionException e) {
                ctx.getErrorCollector().addError("executing sequence => waiting for feature to be sent => exception during execution", e.getCause(), calculation.getFeature(), formula);
                success.failed();
            } catch (Throwable e) {
                ctx.getErrorCollector().addError("executing sequence => waiting for feature to be sent => exception during execution", e, calculation.getFeature(), formula);
                success.failed();
            }
        }

        log(logger, ctx, "[S=%s] All calculations send to script engine", currentSequence);

        // C. wait (block !) for output to be received from the ScriptEngine
        for (var calculation : calculations) {
        	Formula formula = ctx.getProtocolData().formulas.get(calculation.getFeature().getFormulaId());
            try {
                calculation.waitForOutput();
            } catch (InterruptedException e) {
                ctx.getErrorCollector().addError("executing sequence => waiting for output to be received => interrupted", e, calculation.getFeature(), formula);
                success.failed();
            } catch (ExecutionException e) {
                ctx.getErrorCollector().addError("executing sequence => waiting for output to be received => exception during execution", e.getCause(), calculation.getFeature(), formula);
                success.failed();
            } catch (Throwable e) {
                ctx.getErrorCollector().addError("executing sequence => waiting for output to be received => exception during execution", e, calculation.getFeature(), formula);
                success.failed();
            }
        }

        log(logger, ctx, "[S=%s] All outputs received from script engine", currentSequence);
        success.setResult(calculations);
        return success;
    }

    public Optional<ResultDataDTO> emitFeatureCalculationOutput(CalculationContext ctx, FeatureCalculation calculation) {
        if (calculation.getOutput().isEmpty()) {
            return Optional.empty();
        }
        
        var feature = calculation.getFeature();
        var formula = ctx.getProtocolData().formulas.get(feature.getFormulaId());
        var output = calculation.getOutput().get();
        
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
                    // Emit result data event
                    var resultData = ResultDataDTO.builder()
                            .resultSetId(ctx.getResultSetId())
                            .featureId(feature.getId())
                            .values(floatOutputValue)
                            .statusCode(modelMapper.map(output.getStatusCode()))
                            .statusMessage(output.getStatusMessage())
                            .exitCode(output.getExitCode())
                            .build();
                    kafkaProducerService.sendResultData(resultData);

                    // Initiate a curve fitting event
                    var curveFitRequest = new CurveFittingRequestDTO(ctx.getPlate().getId(), resultData.getFeatureId(), resultData);
                    kafkaProducerService.initiateCurveFitting(curveFitRequest);

                    return Optional.of(resultData);
                } catch (JsonProcessingException e) {
                    ctx.getErrorCollector().addError("executing sequence => processing output => parsing output", e, feature, output, formula);
                }
            } else {
            	// Emit result data event
                var resultData = ResultDataDTO.builder()
                        .resultSetId(ctx.getResultSetId())
                        .featureId(feature.getId())
                        .values(new float[]{})
                        .statusCode(modelMapper.map(output.getStatusCode()))
                        .statusMessage(output.getStatusMessage())
                        .exitCode(output.getExitCode())
                        .build();
                kafkaProducerService.sendResultData(resultData);

                // Initiate a curve fitting event
                var curveFitRequest = new CurveFittingRequestDTO(ctx.getPlate().getId(), resultData.getFeatureId(), resultData);
                kafkaProducerService.initiateCurveFitting(curveFitRequest);

                ctx.getErrorCollector().addError(String.format("executing sequence => processing output => output indicates error [%s]", output.getStatusCode()), output, feature, formula);
                return Optional.of(resultData);
            }
        } catch (Exception e) {
            ctx.getErrorCollector().addError("executing sequence => processing output => saving resultdata", e, feature, formula);
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
        private final FeatureDTO feature;

        private Optional<ScriptExecution> scriptExecution = Optional.empty();
        private Optional<ScriptExecutionOutputDTO> output = Optional.empty();

        public FeatureCalculation(FeatureDTO feature, Future<Optional<ScriptExecution>> scriptExecutionFuture) {
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

        public FeatureDTO getFeature() {
            return feature;
        }

        public Optional<ScriptExecutionOutputDTO> getOutput() {
            return output;
        }
    }
}
