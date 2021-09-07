package eu.openanalytics.phaedra.calculationservice.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.openanalytics.phaedra.calculationservice.controller.clients.MeasServiceClient;
import eu.openanalytics.phaedra.calculationservice.controller.clients.MeasUnresolvableException;
import eu.openanalytics.phaedra.calculationservice.enumeration.CalculationScope;
import eu.openanalytics.phaedra.calculationservice.enumeration.Category;
import eu.openanalytics.phaedra.calculationservice.enumeration.ScriptLanguage;
import eu.openanalytics.phaedra.calculationservice.model.Feature;
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
    private final MeasServiceClient measServiceClient;
    private final ResultDataServiceClient resultDataServiceClient;
    private final ObjectMapper objectMapper = new ObjectMapper(); // TODO thread-safe?

    public FeatureExecutorService(ScriptEngineClient scriptEngineClient, MeasServiceClient measServiceClient, ResultDataServiceClient resultDataServiceClient) {
        this.scriptEngineClient = scriptEngineClient;
        this.measServiceClient = measServiceClient;
        this.resultDataServiceClient = resultDataServiceClient;
    }


    public Optional<ScriptExecution> executeFeature(ErrorCollector errorCollector, Feature feature, long measId, long currentSequence, long resultId) {
        try {
            var inputVariables = collectVariablesForFeature(errorCollector, feature, measId, currentSequence, resultId);
            if (inputVariables.isEmpty()) {
                return Optional.empty();
            }
            if (feature.getFormula().getCategory() != Category.CALCULATION
                    || feature.getFormula().getLanguage() != ScriptLanguage.R
                    || feature.getFormula().getScope() != CalculationScope.WELL) {
                errorCollector.handleError("executing feature => unsupported formula found", feature);
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
            errorCollector.handleError(e, "executing feature => writing input variables and request", feature);
        }
        return Optional.empty();
    }

    private Optional<HashMap<String, float[]>> collectVariablesForFeature(ErrorCollector errorCollector, Feature feature, long measId, long currentSequence, long resultId) {
        var inputVariables = new HashMap<String, float[]>();

        for (var civ : feature.getCalculationInputValues()) {
            try {
                if (inputVariables.containsKey(civ.getVariableName())) {
                    // the ProtocolService makes sure this cannot happen, but extra check to make sure
                    errorCollector.handleError("executing sequence => executing feature => collecting variables for feature => duplicate variable name detected", feature, civ);
                    return Optional.empty();
                }

                if (civ.getSourceFeatureId() != null) {
                    if (currentSequence == 0) {
                        errorCollector.handleError("executing sequence => executing feature => collecting variables for feature => retrieving measurement => trying to get feature in sequence 0", feature, civ);
                        return Optional.empty();
                    }
                    inputVariables.put(civ.getVariableName(), resultDataServiceClient.getResultData(resultId, civ.getSourceFeatureId()).getValues());
                } else if (civ.getSourceMeasColName() != null) {
                    inputVariables.put(civ.getVariableName(), measServiceClient.getWellData(measId, civ.getSourceMeasColName()));
                } else {
                    // the ProtocolService makes sure this cannot happen, but extra check to make sure
                    errorCollector.handleError("executing sequence => executing feature => collecting variables for feature => retrieving measurement => civ has no valid source", feature, civ);
                    return Optional.empty();
                }

            } catch (MeasUnresolvableException | ResultDataUnresolvableException e) {
                errorCollector.handleError(e, "executing sequence => executing feature => collecting variables for feature => retrieving measurement", feature, civ);
                return Optional.empty();
            }
        }
        return Optional.of(inputVariables);
    }
}

