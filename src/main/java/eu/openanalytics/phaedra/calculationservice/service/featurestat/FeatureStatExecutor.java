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
import eu.openanalytics.phaedra.platservice.client.exception.PlateUnresolvableException;
import eu.openanalytics.phaedra.platservice.dto.WellDTO;
import eu.openanalytics.phaedra.resultdataservice.client.ResultDataServiceClient;
import eu.openanalytics.phaedra.resultdataservice.dto.ResultDataDTO;
import eu.openanalytics.phaedra.resultdataservice.enumeration.StatusCode;
import eu.openanalytics.phaedra.scriptengine.client.ScriptEngineClient;
import eu.openanalytics.phaedra.scriptengine.client.model.ScriptExecution;
import eu.openanalytics.phaedra.scriptengine.dto.ScriptExecutionOutputDTO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

import static eu.openanalytics.phaedra.calculationservice.CalculationService.JAVASTAT_FAST_LANE;

@Service
public class FeatureStatExecutor {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private final PlateServiceClient plateServiceClient;
    private final ScriptEngineClient scriptEngineClient;
    private final ObjectMapper objectMapper;
    private final ResultDataServiceClient resultDataServiceClient;
    private final ModelMapper modelMapper;

    public FeatureStatExecutor(PlateServiceClient plateServiceClient, ScriptEngineClient scriptEngineClient, ObjectMapper objectMapper, ResultDataServiceClient resultDataServiceClient, ModelMapper modelMapper) {
        this.plateServiceClient = plateServiceClient;
        this.scriptEngineClient = scriptEngineClient;
        this.objectMapper = objectMapper;
        this.resultDataServiceClient = resultDataServiceClient;
        this.modelMapper = modelMapper;
    }

    public Boolean executeFeatureStat(CalculationContext cctx, Feature feature, ResultDataDTO resultData) {
        if (!Objects.equals(resultData.getFeatureId(), feature.getId())) {
            logger.warn("FeatureId must match the FeatureId of the provided Resultdata");
            return false;
        }

        if (resultData.getStatusCode() != StatusCode.SUCCESS) {
            logger.info("Skipping calculating all FeatureStats for Feature because the resultData indicates an error");
            return false;
        }

        try {
            // 1. get wells of plate
            var wellsSorted = plateServiceClient.getWellsOfPlateSorted(cctx.plate().getId());
            var welltypesSorted = wellsSorted.stream().map(WellDTO::getWelltype).toList();

            var calculations = new ArrayList<FeatureStatCalculation>();

            // 2. send Calculations to ScriptEngine (we do this synchronous, because no API/DB queries are needed)
            for (var featureStat : feature.getFeatureStats()) {
                // A. get formula
                var formula = featureStat.getFormula();

                // B. validate it
                if (formula.getCategory() != Category.CALCULATION
                        || formula.getLanguage() != ScriptLanguage.JAVASTAT) {
//                errorCollector.handleError("executing feature => unsupported formula found", feature);
                    logger.info("Skipping FeatureStat because the formula is not valid.");
                    continue;
                }

                // C. prepare input
                var input = new HashMap<String, Object>() {{
                    put("lowWelltype", cctx.protocol().getLowWelltype());
                    put("highWelltype", cctx.protocol().getHighWelltype());
                    put("welltypes", welltypesSorted);
                    put("featureValues", resultData.getValues());
                    put("isPlateStat", featureStat.getPlateStat());
                    put("isWelltypeStat", featureStat.getWelltypeStat());
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
                    cctx.errorCollector().handleError("executing featureStat => writing input variables and request", e, feature, featureStat, featureStat.getFormula());
                }
            }

            // 3. collect output
            for (var calculation : calculations) {
                try {
                    calculation.waitForOutput();
                } catch (InterruptedException e) {
                    cctx.errorCollector().handleError("executing featureStat => waiting for output to be received => interrupted", e, feature, calculation.getFeatureStat(), calculation.getFeatureStat().getFormula());
                } catch (ExecutionException e) {
                    cctx.errorCollector().handleError("executing featureStat => waiting for output to be received => exception during execution", e.getCause(), feature, calculation.getFeatureStat(), calculation.getFeatureStat().getFormula());
                } catch (Throwable e) {
                    cctx.errorCollector().handleError("executing featureStat => waiting for output to be received => exception during execution", e, feature, calculation.getFeatureStat(), calculation.getFeatureStat().getFormula());
                }
            }

            for (var calculation : calculations) {
                if (calculation.getOutput().isEmpty()) {
                    continue;
                }

                var output = calculation.getOutput().get();
                var featureStat = calculation.getFeatureStat();
                switch (output.getStatusCode()) {
                    case SUCCESS -> {
                        try {
                            var outputValues = objectMapper.readValue(output.getOutput(), OutputWrapper.class);
                            var plateValue = outputValues.getPlateValue();
                            if (plateValue.isPresent()) {
                                resultDataServiceClient.createResultFeatureStat(
                                        cctx.resultSetId(),
                                        feature.getId(),
                                        featureStat.getId(),
                                        plateValue.get(),
                                        featureStat.getName(),
                                        null,
                                        modelMapper.map(output.getStatusCode()),
                                        output.getStatusMessage(),
                                        output.getExitCode());
                            }

                            var wellTypeValues = outputValues.getWelltypeOutputs();
                            for (var wellTypeValue : wellTypeValues.entrySet()) {
                                resultDataServiceClient.createResultFeatureStat(
                                        cctx.resultSetId(),
                                        feature.getId(),
                                        featureStat.getId(),
                                        wellTypeValue.getValue(),
                                        featureStat.getName(),
                                        wellTypeValue.getKey(),
                                        modelMapper.map(output.getStatusCode()),
                                        output.getStatusMessage(),
                                        output.getExitCode());
                            }
                        } catch (JsonProcessingException e) {
                            cctx.errorCollector().handleError("executing featureStat => processing output => parsing output", feature, featureStat, featureStat.getFormula(), output);
                        } catch (Exception e) {
                            cctx.errorCollector().handleError("executing featureStat  => processing output => saving resultdata", feature, featureStat, featureStat.getFormula());
                        }
                    }
                    case BAD_REQUEST -> cctx.errorCollector().handleError("executing featureStat => processing output => output indicates bad request", feature, featureStat, featureStat.getFormula());
                    case SCRIPT_ERROR -> cctx.errorCollector().handleError("executing featureStat => processing output => output indicates script error", feature, featureStat, featureStat.getFormula());
                    case WORKER_INTERNAL_ERROR -> {
                        // TODO re-schedule script?
                    }
                }
            }

            return true;
        } catch (PlateUnresolvableException e) {
            cctx.errorCollector().handleError("executing featureStat => fetching wells => exception", feature);
            return false;
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
