package eu.openanalytics.phaedra.calculationservice.service.featurestat;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.openanalytics.phaedra.calculationservice.enumeration.Category;
import eu.openanalytics.phaedra.calculationservice.enumeration.ScriptLanguage;
import eu.openanalytics.phaedra.calculationservice.model.CalculationContext;
import eu.openanalytics.phaedra.calculationservice.model.Feature;
import eu.openanalytics.phaedra.calculationservice.model.FeatureStat;
import eu.openanalytics.phaedra.calculationservice.service.ModelMapper;
import eu.openanalytics.phaedra.platservice.client.PlateServiceClient;
import eu.openanalytics.phaedra.resultdataservice.client.ResultDataServiceClient;
import eu.openanalytics.phaedra.resultdataservice.client.exception.ResultFeatureStatUnresolvableException;
import eu.openanalytics.phaedra.resultdataservice.dto.ResultDataDTO;
import eu.openanalytics.phaedra.resultdataservice.dto.ResultFeatureStatDTO;
import eu.openanalytics.phaedra.resultdataservice.enumeration.StatusCode;
import eu.openanalytics.phaedra.scriptengine.client.ScriptEngineClient;
import eu.openanalytics.phaedra.scriptengine.client.model.ScriptExecution;
import eu.openanalytics.phaedra.scriptengine.dto.ScriptExecutionOutputDTO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

import static eu.openanalytics.phaedra.calculationservice.CalculationService.JAVASTAT_FAST_LANE;
import static eu.openanalytics.phaedra.calculationservice.service.protocol.ProtocolLogger.log;

@Service
public class FeatureStatExecutor {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private final ScriptEngineClient scriptEngineClient;
    private final ObjectMapper objectMapper;
    private final ResultDataServiceClient resultDataServiceClient;
    private final ModelMapper modelMapper;

    public FeatureStatExecutor(PlateServiceClient plateServiceClient, ScriptEngineClient scriptEngineClient, ObjectMapper objectMapper, ResultDataServiceClient resultDataServiceClient, ModelMapper modelMapper) {
        this.scriptEngineClient = scriptEngineClient;
        this.objectMapper = objectMapper;
        this.resultDataServiceClient = resultDataServiceClient;
        this.modelMapper = modelMapper;
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

        var success = true;

        log(logger, cctx, "[F=%s] Calculating FeatureStats", feature.getId());

        // 1. send Calculations to ScriptEngine (we do this synchronous, because no API/DB queries are needed)
        final var calculations = new ArrayList<FeatureStatCalculation>();
        final var resultFeatureStats = new ArrayList<ResultFeatureStatDTO>();
        for (var featureStat : feature.getFeatureStats()) {
            // A. get formula
            var formula = featureStat.getFormula();

            // B. validate it
            if (formula.getCategory() != Category.CALCULATION
                    || formula.getLanguage() != ScriptLanguage.JAVASTAT) {
                cctx.getErrorCollector().handleError("Skipping calculating FeatureStat because the formula is not valid (category must be CALCULATION, language must be JAVASTAT)",
                        feature, featureStat, formula);
                convertErrorOutput(resultFeatureStats, cctx, feature, featureStat, "CalculationService detected an invalid formula");
                success = false;
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

        // 2. collect output
        for (var calculation : calculations) {
            try {
                calculation.waitForOutput();
            } catch (InterruptedException e) {
                cctx.getErrorCollector().handleError("executing featureStat => waiting for output to be received => interrupted", e, feature, calculation.getFeatureStat(), calculation.getFeatureStat().getFormula());
                success = false;
            } catch (ExecutionException e) {
                cctx.getErrorCollector().handleError("executing featureStat => waiting for output to be received => exception during execution", e.getCause(), feature, calculation.getFeatureStat(), calculation.getFeatureStat().getFormula());
                success = false;
            } catch (Throwable e) {
                cctx.getErrorCollector().handleError("executing featureStat => waiting for output to be received => exception during execution", e, feature, calculation.getFeatureStat(), calculation.getFeatureStat().getFormula());
                success = false;
            }
        }

        log(logger, cctx, "[F=%s] All FeatureStat output received from script engine", feature.getId());

        // 3. collect output
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
                        success = false;
                    }
                }
                case BAD_REQUEST -> {
                    cctx.getErrorCollector().handleError("executing featureStat => processing output => output indicates bad request", feature, featureStat, featureStat.getFormula());
                    convertErrorOutput(resultFeatureStats, cctx, feature, featureStat, output);
                    success = false;
                }
                case SCRIPT_ERROR -> {
                    cctx.getErrorCollector().handleError("executing featureStat => processing output => output indicates script error", feature, featureStat, featureStat.getFormula());
                    convertErrorOutput(resultFeatureStats, cctx, feature, featureStat, output);
                    success = false;
                }
                case WORKER_INTERNAL_ERROR -> {
                    // TODO re-schedule script?
                }
            }
        }

        // 4. store output
        try {
            resultDataServiceClient.createResultFeatureStats(cctx.getResultSetId(), resultFeatureStats);
        } catch (ResultFeatureStatUnresolvableException e) {
            cctx.getErrorCollector().handleError("executing featureStat => processing output => saving resultdata", e, feature);
            success = false;
        }

        log(logger, cctx, "[F=%s] All FeatureStat output saved", feature.getId());
        return success;
    }

    /**
     * Save output in case of a succesful calculation.
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
                    .resultSetId(cctx.getResultSetId())
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
                        .resultSetId(cctx.getResultSetId())
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
                    .resultSetId(cctx.getResultSetId())
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
                        .resultSetId(cctx.getResultSetId())
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
                    .resultSetId(cctx.getResultSetId())
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
                        .resultSetId(cctx.getResultSetId())
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
