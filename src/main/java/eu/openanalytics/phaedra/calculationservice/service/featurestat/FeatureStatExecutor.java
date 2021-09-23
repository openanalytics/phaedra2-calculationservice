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
import eu.openanalytics.phaedra.resultdataservice.client.exception.ResultFeatureStatUnresolvableException;
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

    public void executeFeatureStat(CalculationContext cctx, Feature feature, ResultDataDTO resultData) throws PlateUnresolvableException, JsonProcessingException, ResultFeatureStatUnresolvableException {
        if (!Objects.equals(resultData.getFeatureId(), feature.getId())) {
            throw new IllegalArgumentException("FeatureId must match the FeatureId of the provided Resultdata");
        }

        if (resultData.getStatusCode() != StatusCode.SUCCESS) {
            logger.info("Skipping calculating all FeatureStats for Feature because the resultData indicates an error");
            return;
        }

        // 1. get wells of plate
        var wellsSorted = plateServiceClient.getWellsOfPlateSorted(cctx.plate().getId());
        var welltypesSorted = wellsSorted.stream().map(WellDTO::getWelltype).toList();

        var calculations = new ArrayList<FeatureStatCalculation>();

        // 2. send Calculations to ScriptEngine (we do this synchronuous, because no API/DB queries are needed)
        for (var featureStat : feature.getFeatureStats()) {
            // 2. get formula
            var formula = featureStat.getFormula();

            // 3. validate it
            if (formula.getCategory() != Category.CALCULATION
                    || formula.getLanguage() != ScriptLanguage.JAVASTAT) {
//                errorCollector.handleError("executing feature => unsupported formula found", feature);
                logger.info("Skipping FeatureStat because the formula is not valid.");
                continue;
            }

            // 4. prepare input
            var input = new HashMap<String, Object>() {{
                put("lowWelltype", cctx.protocol().getLowWelltype());
                put("highWelltype", cctx.protocol().getHighWelltype());
                put("welltypes", welltypesSorted);
                put("featureValues", resultData.getValues());
            }};

            // 4. send it to the ScriptEngine
            var script = formula.getFormula();

            var execution = scriptEngineClient.newScriptExecution(
                    JAVASTAT_FAST_LANE,
                    script,
                    objectMapper.writeValueAsString(input)
            );

            scriptEngineClient.execute(execution);

            calculations.add(new FeatureStatCalculation(featureStat, execution));
        }

        // 3. collect output
        for (var calculation : calculations) {
            try {
                calculation.waitForOutput();
//                outputs.add(calculation); // the calculation is only added when no excpetion is thrown
            } catch (InterruptedException e) {
                e.printStackTrace();
//                errorCollector.handleError(e, "executing sequence => waiting for feature to be sent => interrupted", calculation.getLeft());
            } catch (ExecutionException e) {
                e.printStackTrace();
//                errorCollector.handleError(e.getCause(), "executing sequence => waiting for feature to be sent => exception during execution", calculation.getLeft());
            } catch (Throwable e) {
//                errorCollector.handleError(e, "executing sequence => waiting for feature to be sent => exception during execution", calculation.getLeft());
                e.printStackTrace();
            }
        }

        for (var calculation : calculations) {
            if (calculation.getOutput().isEmpty()) {
                continue;
            }

            var output = calculation.getOutput().get();
            var featureStat = calculation.getFeatureStat();
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
