/**
 * Phaedra II
 * <p>
 * Copyright (C) 2016-2023 Open Analytics
 * <p>
 * ===========================================================================
 * <p>
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the Apache License as published by
 * The Apache Software Foundation, either version 2 of the License, or
 * (at your option) any later version.
 * <p>
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Apache License for more details.
 * <p>
 * You should have received a copy of the Apache License
 * along with this program.  If not, see <http://www.apache.org/licenses/>
 */
package eu.openanalytics.phaedra.calculationservice.service.featurestat;

import static eu.openanalytics.phaedra.calculationservice.CalculationService.JAVASTAT_FAST_LANE;
import static eu.openanalytics.phaedra.calculationservice.service.protocol.ProtocolLogger.log;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

import eu.openanalytics.phaedra.calculationservice.service.KafkaProducerService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import eu.openanalytics.phaedra.calculationservice.enumeration.Category;
import eu.openanalytics.phaedra.calculationservice.enumeration.ScriptLanguage;
import eu.openanalytics.phaedra.calculationservice.model.CalculationContext;
import eu.openanalytics.phaedra.calculationservice.model.Feature;
import eu.openanalytics.phaedra.calculationservice.model.FeatureStat;
import eu.openanalytics.phaedra.calculationservice.model.SuccessTracker;
import eu.openanalytics.phaedra.calculationservice.service.ModelMapper;
import eu.openanalytics.phaedra.resultdataservice.client.ResultDataServiceClient;
import eu.openanalytics.phaedra.resultdataservice.client.exception.ResultFeatureStatUnresolvableException;
import eu.openanalytics.phaedra.resultdataservice.dto.ResultDataDTO;
import eu.openanalytics.phaedra.resultdataservice.dto.ResultFeatureStatDTO;
import eu.openanalytics.phaedra.resultdataservice.enumeration.StatusCode;
import eu.openanalytics.phaedra.scriptengine.client.ScriptEngineClient;
import eu.openanalytics.phaedra.scriptengine.client.model.ScriptExecution;
import eu.openanalytics.phaedra.scriptengine.dto.ScriptExecutionOutputDTO;

@Service
public class FeatureStatExecutor {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private final ScriptEngineClient scriptEngineClient;
    private final ObjectMapper objectMapper;
    private final ResultDataServiceClient resultDataServiceClient;
    private final ModelMapper modelMapper;
    private final KafkaProducerService kafkaProducerService;

    private final static int MAX_ATTEMPTS = 3;

    public FeatureStatExecutor(ScriptEngineClient scriptEngineClient, ObjectMapper objectMapper,
                               ResultDataServiceClient resultDataServiceClient, ModelMapper modelMapper,
                               KafkaProducerService kafkaProducerService) {
        this.scriptEngineClient = scriptEngineClient;
        this.objectMapper = objectMapper;
        this.resultDataServiceClient = resultDataServiceClient;
        this.modelMapper = modelMapper;
        this.kafkaProducerService = kafkaProducerService;
    }

    public Boolean executeFeatureStat(CalculationContext cctx, Feature feature, ResultDataDTO resultData) {

        if (!Objects.equals(resultData.getFeatureId(), feature.getId())) {
            cctx.getErrorCollector().handleError("Skipping calculating FeatureStats because FeatureId does not match the FeatureId of the ResultData", feature);
            return false;
        }

        if (resultData.getStatusCode() != StatusCode.SUCCESS) {
            cctx.getErrorCollector().handleError("Skipping calculating FeatureStats because the ResultData indicates an error", feature);
            return false;
        }

        var success = new SuccessTracker<Void>();

        log(logger, cctx, "[F=%s] Calculating FeatureStats", feature.getId());

        // 1. try to calculate all FeatureStats
        var featureStatsToCalculate = feature.getFeatureStats();
        final var featureStatCount = featureStatsToCalculate.size(); // total number of featureStats to calculate
        var calculations = new ArrayList<FeatureStatCalculation>();
        var resultFeatureStats = new ArrayList<ResultFeatureStatDTO>();

        for (int attempt = 1; true; attempt++) {
            log(logger, cctx, "[F=%s] Attempt %s to calculate [%s of %s] featureStats", feature.getId(), attempt, featureStatsToCalculate.size(), featureStatCount);
            var currentAttempt = calculateFeatureStats(cctx, feature, featureStatsToCalculate, resultData, resultFeatureStats);
            success.failedIfStepFailed(currentAttempt);

            if (!currentAttempt.isSuccess() || attempt == MAX_ATTEMPTS) {
                // 2. there was a hard-error or we are at the last attempt -> do not re-attempt any calculation
                calculations.addAll(currentAttempt.getResult());
                break;
            }

            // 3. check if we have to retry any of the calculations
            var featuresStatsToRetry = new ArrayList<FeatureStat>();
            for (var calculation : currentAttempt.getResult()) {
                if (calculation.getOutput().isPresent() && calculation.getOutput().get().getStatusCode().canBeRetried()) {
                    featuresStatsToRetry.add(calculation.getFeatureStat());
                } else {
                    calculations.add(calculation);
                }
            }
            if (featuresStatsToRetry.size() == 0) {
                // 4. nothing to retry
                break;
            }
            featureStatsToCalculate = featuresStatsToRetry;
        }

        // 5. collect output
        for (var calculation : calculations) {
            var featureStat = calculation.getFeatureStat();
            if (calculation.getOutput().isEmpty()) {
                convertErrorOutput(resultFeatureStats, cctx, feature, featureStat, "CalculationService was unable to process the calculation");
                continue;
            }

            var output = calculation.getOutput().get();
            switch (output.getStatusCode()) {
                case SUCCESS -> {
                    try {
                        convertOutput(resultFeatureStats, cctx, feature, featureStat, output);
                    } catch (JsonProcessingException e) {
                        cctx.getErrorCollector().handleError("executing featureStat => processing output => parsing output", e, feature, featureStat, featureStat.getFormula(), output);
                        success.failed();
                    }
                }
                case BAD_REQUEST, SCRIPT_ERROR, WORKER_INTERNAL_ERROR, RESCHEDULED_BY_WATCHDOG -> {
                    cctx.getErrorCollector().handleError(String.format("executing featureStat => processing output => output indicates error [%s]", output.getStatusCode()), feature, featureStat, featureStat.getFormula());
                    convertErrorOutput(resultFeatureStats, cctx, feature, featureStat, output);
                    success.failed();
                }
            }
        }

        // 6. store output
//        try {
//            resultDataServiceClient.createResultFeatureStats(cctx.getResultSetId(), resultFeatureStats);
            kafkaProducerService.sendResultFeatureStats(cctx.getResultSetId(), resultFeatureStats);

//        } catch (ResultFeatureStatUnresolvableException e) {
//            cctx.getErrorCollector().handleError("executing featureStat => processing output => saving resultdata", e, feature);
//            success.failed();
//        }

        log(logger, cctx, "[F=%s] All FeatureStat output saved", feature.getId());
        return success.isSuccess();
    }

    private SuccessTracker<List<FeatureStatCalculation>> calculateFeatureStats(CalculationContext cctx, Feature feature,
                                                                               List<FeatureStat> featureStats,
                                                                               ResultDataDTO resultData,
                                                                               ArrayList<ResultFeatureStatDTO> resultFeatureStats) {
        var success = new SuccessTracker<List<FeatureStatCalculation>>();
        // 1. send Calculations to ScriptEngine (we do this synchronous, because no API/DB queries are needed)
        final var calculations = new ArrayList<FeatureStatCalculation>();
        for (var featureStat : featureStats) {
            // A. get formula
            var formula = featureStat.getFormula();

            // B. validate it
            if (formula.getCategory() != Category.CALCULATION
                    || formula.getLanguage() != ScriptLanguage.JAVASTAT) {
                cctx.getErrorCollector().handleError("Skipping calculating FeatureStat because the formula is not valid (category must be CALCULATION, language must be JAVASTAT)",
                        feature, featureStat, formula);
                convertErrorOutput(resultFeatureStats, cctx, feature, featureStat, "CalculationService detected an invalid formula");
                success.failed();
                continue;
            }

            // C. prepare input
            var input = new HashMap<String, Object>() {{
                put("lowWelltype", cctx.getProtocol().getLowWelltype());
                put("highWelltype", cctx.getProtocol().getHighWelltype());
                put("welltypes", cctx.getWelltypesSorted());
                put("featureValues", resultData.getValues());
                put("isPlateStat", featureStat.isPlateStat());
                put("isWelltypeStat", featureStat.isWelltypeStat());
            }};

            // D. send it to the ScriptEngine
            var script = formula.getFormula();

            try {
                var execution = scriptEngineClient.newScriptExecution(
                        JAVASTAT_FAST_LANE,
                        script,
                        objectMapper.writeValueAsString(input)
                );

                scriptEngineClient.execute(execution);
                calculations.add(new FeatureStatCalculation(featureStat, execution));
            } catch (JsonProcessingException e) {
                // this error will probably never occur, see: https://stackoverflow.com/q/26716020/1393103 for examples where it does
                cctx.getErrorCollector().handleError("executing featureStat => writing input variables and request", e, feature, featureStat, featureStat.getFormula());
            }
        }

        log(logger, cctx, "[F=%s] All FeatureStat calculations send to script engine", feature.getId());

        // 2. wait for output to be received
        for (var calculation : calculations) {
            try {
                calculation.waitForOutput();
            } catch (InterruptedException e) {
                cctx.getErrorCollector().handleError("executing featureStat => waiting for output to be received => interrupted", e, feature, calculation.getFeatureStat(), calculation.getFeatureStat().getFormula());
                success.failed();
            } catch (ExecutionException e) {
                cctx.getErrorCollector().handleError("executing featureStat => waiting for output to be received => exception during execution", e.getCause(), feature, calculation.getFeatureStat(), calculation.getFeatureStat().getFormula());
                success.failed();
            } catch (Throwable e) {
                cctx.getErrorCollector().handleError("executing featureStat => waiting for output to be received => exception during execution", e, feature, calculation.getFeatureStat(), calculation.getFeatureStat().getFormula());
                success.failed();
            }
        }

        log(logger, cctx, "[F=%s] All FeatureStat output received from script engine", feature.getId());

        success.setResult(calculations);
        return success;
    }


    /**
     * Save output in case of a successful calculation.
     */
    private void convertOutput(List<ResultFeatureStatDTO> res, CalculationContext cctx, Feature feature, FeatureStat featureStat, ScriptExecutionOutputDTO output) throws JsonProcessingException {
        var outputValues = objectMapper.readValue(output.getOutput(), OutputWrapper.class);
        var statusCode = modelMapper.map(output.getStatusCode());
        var plateValue = outputValues.getPlateValue();

        if (featureStat.isPlateStat()) {
            if (plateValue.isEmpty()) {
                cctx.getErrorCollector().handleError("executing featureStat => processing output => expected to receive a plateValue but did not receive it", feature, featureStat, featureStat.getFormula(), output);
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

        if (featureStat.isWelltypeStat()) {
            var wellTypeValues = outputValues.getWelltypeOutputs();
            for (var welltype : cctx.getUniqueWelltypes()) {
                var value = wellTypeValues.get(welltype);
                if (value == null) {
                    cctx.getErrorCollector().handleError(String.format("executing featureStat => processing output => expected to receive a result for welltype [%s] but did not receive it", welltype), feature, featureStat, featureStat.getFormula(), output);
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
    private void convertErrorOutput(List<ResultFeatureStatDTO> res, CalculationContext cctx, Feature feature, FeatureStat featureStat, ScriptExecutionOutputDTO output) {
        var statusCode = modelMapper.map(output.getStatusCode());

        if (featureStat.isPlateStat()) {
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

        if (featureStat.isWelltypeStat()) {
            for (var welltype : cctx.getUniqueWelltypes()) {
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
    private void convertErrorOutput(List<ResultFeatureStatDTO> res, CalculationContext cctx, Feature feature, FeatureStat featureStat, String statusMessage) {
        if (featureStat.isPlateStat()) {
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

        if (featureStat.isWelltypeStat()) {
            for (var welltype : cctx.getUniqueWelltypes()) {
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
        private final FeatureStat featureStat;

        private Optional<ScriptExecutionOutputDTO> output = Optional.empty();

        public Optional<ScriptExecutionOutputDTO> getOutput() {
            return output;
        }

        public FeatureStatCalculation(FeatureStat featureStat, ScriptExecution scriptExecution) {
            Objects.requireNonNull(featureStat, "FeatureStat cannot be null");
            Objects.requireNonNull(scriptExecution, "ScriptExecution cannot be null");
            this.featureStat = featureStat;
            this.scriptExecution = scriptExecution;
        }

        public void waitForOutput() throws ExecutionException, InterruptedException {
            output = Optional.of(scriptExecution.getOutput().get());
        }

        public FeatureStat getFeatureStat() {
            return featureStat;
        }

    }

}
