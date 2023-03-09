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

import static eu.openanalytics.phaedra.calculationservice.CalculationService.JAVASTAT_FAST_LANE;
import static eu.openanalytics.phaedra.calculationservice.util.LoggerHelper.log;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import eu.openanalytics.phaedra.calculationservice.enumeration.FormulaCategory;
import eu.openanalytics.phaedra.calculationservice.enumeration.ScriptLanguage;
import eu.openanalytics.phaedra.calculationservice.model.CalculationContext;
import eu.openanalytics.phaedra.calculationservice.model.ModelMapper;
import eu.openanalytics.phaedra.calculationservice.service.KafkaProducerService;
import eu.openanalytics.phaedra.calculationservice.util.SuccessTracker;
import eu.openanalytics.phaedra.plateservice.dto.WellDTO;
import eu.openanalytics.phaedra.protocolservice.dto.FeatureDTO;
import eu.openanalytics.phaedra.protocolservice.dto.FeatureStatDTO;
import eu.openanalytics.phaedra.resultdataservice.dto.ResultDataDTO;
import eu.openanalytics.phaedra.resultdataservice.dto.ResultFeatureStatDTO;
import eu.openanalytics.phaedra.resultdataservice.enumeration.StatusCode;
import eu.openanalytics.phaedra.scriptengine.client.ScriptEngineClient;
import eu.openanalytics.phaedra.scriptengine.client.model.ScriptExecution;
import eu.openanalytics.phaedra.scriptengine.dto.ScriptExecutionOutputDTO;

/**
 * Feature Stats are sets of statistical values about a feature.
 * 
 * Typically this includes min/mean/median/max values for the whole plate,
 * but also stats for each welltype present in the plate.
 * 
 * Feature Stats can be calculated as soon as the Feature itself has been calculated.
 */
@Service
public class FeatureStatExecutorService {

    private final ScriptEngineClient scriptEngineClient;
    private final KafkaProducerService kafkaProducerService;

    private final ObjectMapper objectMapper;
    private final ModelMapper modelMapper;
    private final Logger logger = LoggerFactory.getLogger(getClass());

    private final static int MAX_ATTEMPTS = 3;

    public FeatureStatExecutorService(ScriptEngineClient scriptEngineClient, ObjectMapper objectMapper, ModelMapper modelMapper, KafkaProducerService kafkaProducerService) {
        this.scriptEngineClient = scriptEngineClient;
        this.objectMapper = objectMapper;
        this.modelMapper = modelMapper;
        this.kafkaProducerService = kafkaProducerService;
    }

    public Boolean executeFeatureStat(CalculationContext ctx, FeatureDTO feature, ResultDataDTO resultData) {

        if (!Objects.equals(resultData.getFeatureId(), feature.getId())) {
            ctx.getErrorCollector().addError("Skipping calculating FeatureStats because FeatureId does not match the FeatureId of the ResultData", feature);
            return false;
        }

        if (resultData.getStatusCode() != StatusCode.SUCCESS) {
            ctx.getErrorCollector().addError("Skipping calculating FeatureStats because the ResultData indicates an error", feature);
            return false;
        }

        var success = new SuccessTracker<Void>();
        log(logger, ctx, "[F=%s] Calculating FeatureStats", feature.getId());

        // Try to calculate all FeatureStats
        var featureStatsToCalculate = ctx.getProtocolData().featureStats.get(feature.getId());
        var calculations = new ArrayList<FeatureStatCalculation>();
        var resultFeatureStats = new ArrayList<ResultFeatureStatDTO>();

        for (int attempt = 1; true; attempt++) {
            log(logger, ctx, "[F=%s] Attempt %d to calculate %d featureStats", feature.getId(), attempt, featureStatsToCalculate.size());
            var currentAttempt = calculateFeatureStats(ctx, feature, featureStatsToCalculate, resultData, resultFeatureStats);
            success.failedIfStepFailed(currentAttempt);

            if (!currentAttempt.isSuccess() || attempt == MAX_ATTEMPTS) {
                // There was a hard-error or we are at the last attempt -> do not re-attempt any calculation
                calculations.addAll(currentAttempt.getResult());
                break;
            }

            // Check if we have to retry any of the calculations
            var featuresStatsToRetry = new ArrayList<FeatureStatDTO>();
            for (var calculation : currentAttempt.getResult()) {
                if (calculation.getOutput().isPresent() && calculation.getOutput().get().getStatusCode().canBeRetried()) {
                    featuresStatsToRetry.add(calculation.getFeatureStat());
                } else {
                    calculations.add(calculation);
                }
            }
            if (featuresStatsToRetry.isEmpty()) {
                break;
            }
            featureStatsToCalculate = featuresStatsToRetry;
        }

        // Collect results by blocking until all output is available
        for (var calculation : calculations) {
            var featureStat = calculation.getFeatureStat();
            var formula = ctx.getProtocolData().formulas.get(featureStat.getFormulaId());
            
            if (calculation.getOutput().isEmpty()) {
                convertErrorOutput(resultFeatureStats, ctx, feature, featureStat, "CalculationService was unable to process the calculation");
                continue;
            }

            var output = calculation.getOutput().get();
            switch (output.getStatusCode()) {
                case SUCCESS -> {
                    try {
                        convertOutput(resultFeatureStats, ctx, feature, featureStat, output);
                    } catch (JsonProcessingException e) {
                        ctx.getErrorCollector().addError("executing featureStat => processing output => parsing output", e, feature, featureStat, formula, output);
                        success.failed();
                    }
                }
                case BAD_REQUEST, SCRIPT_ERROR, WORKER_INTERNAL_ERROR, RESCHEDULED_BY_WATCHDOG -> {
                    ctx.getErrorCollector().addError(String.format("executing featureStat => processing output => output indicates error [%s]", output.getStatusCode()), feature, featureStat, formula);
                    convertErrorOutput(resultFeatureStats, ctx, feature, featureStat, output);
                    success.failed();
                }
            }
        }

        // Emit the output
        kafkaProducerService.sendResultFeatureStats(ctx.getResultSetId(), resultFeatureStats);

        log(logger, ctx, "[F=%s] All FeatureStat output saved", feature.getId());
        return success.isSuccess();
    }

    private SuccessTracker<List<FeatureStatCalculation>> calculateFeatureStats(CalculationContext ctx,
    		FeatureDTO feature, List<FeatureStatDTO> featureStats, ResultDataDTO resultData, ArrayList<ResultFeatureStatDTO> resultFeatureStats) {

    	var success = new SuccessTracker<List<FeatureStatCalculation>>();
        
    	// 1. send Calculations to ScriptEngine (we do this synchronous, because no API/DB queries are needed)
        final var calculations = new ArrayList<FeatureStatCalculation>();
        for (var featureStat : featureStats) {
        	var formula = ctx.getProtocolData().formulas.get(featureStat.getFormulaId());
            // Validate it
            if (formula.getCategory() != FormulaCategory.CALCULATION
                    || formula.getLanguage() != ScriptLanguage.JAVASTAT) {
                ctx.getErrorCollector().addError("Skipping calculating FeatureStat because the formula is not valid (category must be CALCULATION, language must be JAVASTAT)",
                        feature, featureStat, formula);
                convertErrorOutput(resultFeatureStats, ctx, feature, featureStat, "CalculationService detected an invalid formula");
                success.failed();
                continue;
            }

            // C. prepare input
            var input = new HashMap<String, Object>();
            input.put("lowWelltype", ctx.getProtocolData().protocol.getLowWelltype());
            input.put("highWelltype", ctx.getProtocolData().protocol.getHighWelltype());
            input.put("welltypes", ctx.getWells().stream().map(WellDTO::getWellType).distinct().sorted().toList());
            input.put("featureValues", resultData.getValues());
            input.put("isPlateStat", featureStat.getPlateStat());
            input.put("isWelltypeStat", featureStat.getWelltypeStat());

            // D. send it to the ScriptEngine
            try {
            	var script = formula.getFormula();
                var execution = scriptEngineClient.newScriptExecution(JAVASTAT_FAST_LANE, script, objectMapper.writeValueAsString(input));
                scriptEngineClient.execute(execution);
                calculations.add(new FeatureStatCalculation(featureStat, execution));
            } catch (JsonProcessingException e) {
                // this error will probably never occur, see: https://stackoverflow.com/q/26716020/1393103 for examples where it does
                ctx.getErrorCollector().addError("executing featureStat => writing input variables and request", e, feature, featureStat);
            }
        }

        log(logger, ctx, "[F=%s] All FeatureStat calculations send to script engine", feature.getId());

        // 2. wait for output to be received
        for (var calculation : calculations) {
        	var formula = ctx.getProtocolData().formulas.get(calculation.getFeatureStat().getFormulaId());
            try {
                calculation.waitForOutput();
            } catch (InterruptedException e) {
                ctx.getErrorCollector().addError("executing featureStat => waiting for output to be received => interrupted", e, feature, calculation.getFeatureStat(), formula);
                success.failed();
            } catch (ExecutionException e) {
                ctx.getErrorCollector().addError("executing featureStat => waiting for output to be received => exception during execution", e.getCause(), feature, calculation.getFeatureStat(), formula);
                success.failed();
            } catch (Throwable e) {
                ctx.getErrorCollector().addError("executing featureStat => waiting for output to be received => exception during execution", e, feature, calculation.getFeatureStat(), formula);
                success.failed();
            }
        }

        log(logger, ctx, "[F=%s] All FeatureStat output received from script engine", feature.getId());

        success.setResult(calculations);
        return success;
    }


    /**
     * Save output in case of a successful calculation.
     */
    private void convertOutput(List<ResultFeatureStatDTO> res, CalculationContext ctx, FeatureDTO feature, FeatureStatDTO featureStat, ScriptExecutionOutputDTO output) throws JsonProcessingException {
        var outputValues = objectMapper.readValue(output.getOutput(), OutputWrapper.class);
        var statusCode = modelMapper.map(output.getStatusCode());
        var plateValue = outputValues.getPlateValue();

        if (featureStat.getPlateStat()) {
            if (plateValue.isEmpty()) {
                ctx.getErrorCollector().addError("executing featureStat => processing output => expected to receive a plateValue but did not receive it", feature, featureStat, output);
            }

            res.add(ResultFeatureStatDTO.builder()
                    .featureId(feature.getId())
                    .featureStatId(featureStat.getId())
                    .value(plateValue.orElse(null))
                    .statisticName(featureStat.getName())
                    .welltype(null)
                    .statusCode(statusCode)
                    .statusMessage(output.getStatusMessage())
                    .exitCode(output.getExitCode()).build());
        }

        if (featureStat.getWelltypeStat()) {
        	var wellTypes = ctx.getWells().stream().map(WellDTO::getWellType).distinct().toList();
            var wellTypeValues = outputValues.getWelltypeOutputs();
            for (var welltype : wellTypes) {
                var value = wellTypeValues.get(welltype);
                if (value == null) {
                    ctx.getErrorCollector().addError(String.format("executing featureStat => processing output => expected to receive a result for welltype [%s] but did not receive it", welltype), feature, featureStat, output);
                }
                res.add(ResultFeatureStatDTO.builder()
                        .featureId(feature.getId())
                        .featureStatId(featureStat.getId())
                        .value(value)
                        .statisticName(featureStat.getName())
                        .welltype(welltype)
                        .statusCode(statusCode)
                        .statusMessage(output.getStatusMessage())
                        .exitCode(output.getExitCode()).build());
            }
        }
    }

    /**
     * Save output in case of an error. Stores `null` as value for the Plate and/or Welltype records.
     */
    private void convertErrorOutput(List<ResultFeatureStatDTO> res, CalculationContext ctx, FeatureDTO feature, FeatureStatDTO featureStat, ScriptExecutionOutputDTO output) {
        var statusCode = modelMapper.map(output.getStatusCode());

        if (featureStat.getPlateStat()) {
            res.add(ResultFeatureStatDTO.builder()
                    .featureId(feature.getId())
                    .featureStatId(featureStat.getId())
                    .value(null)
                    .statisticName(featureStat.getName())
                    .welltype(null)
                    .statusCode(statusCode)
                    .statusMessage(output.getStatusMessage())
                    .exitCode(output.getExitCode()).build());
        }

        if (featureStat.getWelltypeStat()) {
        	var wellTypes = ctx.getWells().stream().map(WellDTO::getWellType).distinct().toList();
            for (var welltype : wellTypes) {
                res.add(ResultFeatureStatDTO.builder()
                        .featureId(feature.getId())
                        .featureStatId(featureStat.getId())
                        .value(null)
                        .statisticName(featureStat.getName())
                        .welltype(welltype)
                        .statusCode(statusCode)
                        .statusMessage(output.getStatusMessage())
                        .exitCode(output.getExitCode())
                        .build());
            }
        }
    }

    /**
     * Save output in case of an error in the CalculationService. Stores `null` as value for the Plate and/or Welltype records.
     */
    private void convertErrorOutput(List<ResultFeatureStatDTO> res, CalculationContext ctx, FeatureDTO feature, FeatureStatDTO featureStat, String statusMessage) {
        if (featureStat.getPlateStat()) {
            res.add(ResultFeatureStatDTO.builder()
                    .featureId(feature.getId())
                    .featureStatId(featureStat.getId())
                    .value(null)
                    .statisticName(featureStat.getName())
                    .welltype(null)
                    .statusCode(StatusCode.FAILURE)
                    .statusMessage(statusMessage)
                    .exitCode(0).build());
        }

        if (featureStat.getWelltypeStat()) {
        	var wellTypes = ctx.getWells().stream().map(WellDTO::getWellType).distinct().toList();
            for (var welltype : wellTypes) {
                res.add(ResultFeatureStatDTO.builder()
                        .featureId(feature.getId())
                        .featureStatId(featureStat.getId())
                        .value(null)
                        .statisticName(featureStat.getName())
                        .welltype(welltype)
                        .statusCode(StatusCode.FAILURE)
                        .statusMessage(statusMessage)
                        .exitCode(0)
                        .build());
            }
        }
    }

    private static class OutputWrapper {
        private final Float plateValue;

        private final Map<String, Float> welltypeValues;

        @JsonCreator
        private OutputWrapper(
                @JsonProperty(value = "plateValue", required = true) Float plateValue,
                @JsonProperty(value = "welltypeValues", required = true) Map<String, Float> welltypeValues) {
            this.plateValue = plateValue;
            this.welltypeValues = welltypeValues;
        }

        public Optional<Float> getPlateValue() {
            return Optional.ofNullable(plateValue);
        }


        public Map<String, Float> getWelltypeOutputs() {
            return welltypeValues;
        }

    }

    private static class FeatureStatCalculation {

        private final ScriptExecution scriptExecution;
        private final FeatureStatDTO featureStat;

        private Optional<ScriptExecutionOutputDTO> output = Optional.empty();

        public Optional<ScriptExecutionOutputDTO> getOutput() {
            return output;
        }

        public FeatureStatCalculation(FeatureStatDTO featureStat, ScriptExecution scriptExecution) {
            Objects.requireNonNull(featureStat, "FeatureStat cannot be null");
            Objects.requireNonNull(scriptExecution, "ScriptExecution cannot be null");
            this.featureStat = featureStat;
            this.scriptExecution = scriptExecution;
        }

        public void waitForOutput() throws ExecutionException, InterruptedException {
            output = Optional.of(scriptExecution.getOutput().get());
        }

        public FeatureStatDTO getFeatureStat() {
            return featureStat;
        }

    }

}
