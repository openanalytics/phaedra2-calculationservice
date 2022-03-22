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

import static eu.openanalytics.phaedra.calculationservice.CalculationService.R_FAST_LANE;

import java.util.HashMap;
import java.util.Optional;

import org.springframework.stereotype.Service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import eu.openanalytics.phaedra.calculationservice.enumeration.CalculationScope;
import eu.openanalytics.phaedra.calculationservice.enumeration.Category;
import eu.openanalytics.phaedra.calculationservice.enumeration.ScriptLanguage;
import eu.openanalytics.phaedra.calculationservice.model.CalculationContext;
import eu.openanalytics.phaedra.calculationservice.model.Feature;
import eu.openanalytics.phaedra.calculationservice.util.CalculationInputHelper;
import eu.openanalytics.phaedra.measurementservice.client.MeasurementServiceClient;
import eu.openanalytics.phaedra.measurementservice.client.exception.MeasUnresolvableException;
import eu.openanalytics.phaedra.resultdataservice.client.ResultDataServiceClient;
import eu.openanalytics.phaedra.resultdataservice.client.exception.ResultDataUnresolvableException;
import eu.openanalytics.phaedra.scriptengine.client.ScriptEngineClient;
import eu.openanalytics.phaedra.scriptengine.client.model.ScriptExecution;

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
                cctx.getErrorCollector().handleError("executing feature => unsupported formula found", feature, feature.getFormula());
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
            cctx.getErrorCollector().handleError("executing feature => writing input variables and request", e, feature, feature.getFormula());
        }
        return Optional.empty();
    }

    private Optional<HashMap<String, Object>> collectVariablesForFeature(CalculationContext cctx, Feature feature, long currentSequence) {
        var inputVariables = new HashMap<String, Object>();

        for (var civ : feature.getCalculationInputValues()) {
            try {
                if (inputVariables.containsKey(civ.getVariableName())) {
                    // the ProtocolService makes sure this cannot happen, but extra check to make sure
                    cctx.getErrorCollector().handleError("executing sequence => executing feature => collecting variables for feature => duplicate variable name detected", feature, feature.getFormula(), civ);
                    return Optional.empty();
                }

                if (civ.getSourceFeatureId() != null) {
                    if (currentSequence == 0) {
                        cctx.getErrorCollector().handleError("executing sequence => executing feature => collecting variables for feature => retrieving measurement => trying to get feature in sequence 0", feature, feature.getFormula(), civ);
                        return Optional.empty();
                    }
                    inputVariables.put(civ.getVariableName(), resultDataServiceClient.getResultData(cctx.getResultSetId(), civ.getSourceFeatureId()).getValues());
                } else if (civ.getSourceMeasColName() != null) {
                    inputVariables.put(civ.getVariableName(), measurementServiceClient.getWellData(cctx.getMeasId(), civ.getSourceMeasColName()));
                } else {
                    // the ProtocolService makes sure this cannot happen, but extra check to make sure
                    cctx.getErrorCollector().handleError("executing sequence => executing feature => collecting variables for feature => retrieving measurement => civ has no valid source", feature, feature.getFormula(), civ);
                    return Optional.empty();
                }

            } catch (MeasUnresolvableException | ResultDataUnresolvableException e) {
                cctx.getErrorCollector().handleError("executing sequence => executing feature => collecting variables for feature => retrieving measurement", e, feature, feature.getFormula(), civ);
                return Optional.empty();
            }
        }
        
        // Add commonly used info about the wells
        CalculationInputHelper.addWellInfo(inputVariables, cctx);
        
        return Optional.of(inputVariables);
    }
}

