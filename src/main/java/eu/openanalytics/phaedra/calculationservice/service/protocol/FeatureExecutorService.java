package eu.openanalytics.phaedra.calculationservice.service.protocol;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.openanalytics.phaedra.calculationservice.enumeration.CalculationScope;
import eu.openanalytics.phaedra.calculationservice.enumeration.Category;
import eu.openanalytics.phaedra.calculationservice.enumeration.ScriptLanguage;
import eu.openanalytics.phaedra.calculationservice.model.CalculationContext;
import eu.openanalytics.phaedra.calculationservice.model.Feature;
import eu.openanalytics.phaedra.measurementservice.client.MeasurementServiceClient;
import eu.openanalytics.phaedra.measurementservice.client.exception.MeasUnresolvableException;
import eu.openanalytics.phaedra.resultdataservice.client.ResultDataServiceClient;
import eu.openanalytics.phaedra.resultdataservice.client.exception.ResultDataUnresolvableException;
import eu.openanalytics.phaedra.scriptengine.client.ScriptEngineClient;
import eu.openanalytics.phaedra.scriptengine.client.model.ScriptExecution;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Optional;

import static eu.openanalytics.phaedra.calculationservice.CalculationService.R_FAST_LANE;

@Service
public class FeatureExecutorService {

    private final ScriptEngineClient scriptEngineClient;
    private final MeasurementServiceClient measurementServiceClient;
    private final ResultDataServiceClient resultDataServiceClient;
    private final ObjectMapper objectMapper = new ObjectMapper(); // TODO thread-safe?

    public FeatureExecutorService(ScriptEngineClient scriptEngineClient, MeasurementServiceClient measurementServiceClient, ResultDataServiceClient resultDataServiceClient) {
        this.scriptEngineClient = scriptEngineClient;
        this.measurementServiceClient = measurementServiceClient;
        this.resultDataServiceClient = resultDataServiceClient;
    }


    public Optional<ScriptExecution> executeFeature(CalculationContext cctx, Feature feature, long currentSequence) {
        try {
            var inputVariables = collectVariablesForFeature(cctx, feature, currentSequence);
            if (inputVariables.isEmpty()) {
                return Optional.empty();
            }
            if (feature.getFormula().getCategory() != Category.CALCULATION
                    || feature.getFormula().getLanguage() != ScriptLanguage.R
                    || feature.getFormula().getScope() != CalculationScope.WELL) {
                cctx.errorCollector().handleError("executing feature => unsupported formula found", feature);
                return Optional.empty();
            }

            var script = feature.getFormula().getFormula();

            var execution = scriptEngineClient.newScriptExecution(
                    R_FAST_LANE,
                    script,
                    objectMapper.writeValueAsString(inputVariables.get())
            );

            scriptEngineClient.execute(execution);

            return Optional.of(execution);
        } catch (JsonProcessingException e) {
            // this error will probably never occur, see: https://stackoverflow.com/q/26716020/1393103 for examples where it does
            cctx.errorCollector().handleError(e, "executing feature => writing input variables and request", feature);
        }
        return Optional.empty();
    }

    private Optional<HashMap<String, float[]>> collectVariablesForFeature(CalculationContext cctx, Feature feature, long currentSequence) {
        var inputVariables = new HashMap<String, float[]>();

        for (var civ : feature.getCalculationInputValues()) {
            try {
                if (inputVariables.containsKey(civ.getVariableName())) {
                    // the ProtocolService makes sure this cannot happen, but extra check to make sure
                    cctx.errorCollector().handleError("executing sequence => executing feature => collecting variables for feature => duplicate variable name detected", feature, civ);
                    return Optional.empty();
                }

                if (civ.getSourceFeatureId() != null) {
                    if (currentSequence == 0) {
                        cctx.errorCollector().handleError("executing sequence => executing feature => collecting variables for feature => retrieving measurement => trying to get feature in sequence 0", feature, civ);
                        return Optional.empty();
                    }
                    inputVariables.put(civ.getVariableName(), resultDataServiceClient.getResultData(cctx.resultSetId(), civ.getSourceFeatureId()).getValues());
                } else if (civ.getSourceMeasColName() != null) {
                    inputVariables.put(civ.getVariableName(), measurementServiceClient.getWellData(cctx.measId(), civ.getSourceMeasColName()));
                } else {
                    // the ProtocolService makes sure this cannot happen, but extra check to make sure
                    cctx.errorCollector().handleError("executing sequence => executing feature => collecting variables for feature => retrieving measurement => civ has no valid source", feature, civ);
                    return Optional.empty();
                }

            } catch (MeasUnresolvableException | ResultDataUnresolvableException e) {
                cctx.errorCollector().handleError(e, "executing sequence => executing feature => collecting variables for feature => retrieving measurement", feature, civ);
                return Optional.empty();
            }
        }
        return Optional.of(inputVariables);
    }
}

