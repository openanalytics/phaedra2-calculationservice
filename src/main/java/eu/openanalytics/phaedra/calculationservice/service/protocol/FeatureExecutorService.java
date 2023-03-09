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

import static eu.openanalytics.phaedra.calculationservice.CalculationService.R_FAST_LANE;

import java.util.HashMap;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import eu.openanalytics.phaedra.calculationservice.enumeration.CalculationScope;
import eu.openanalytics.phaedra.calculationservice.enumeration.FormulaCategory;
import eu.openanalytics.phaedra.calculationservice.enumeration.ScriptLanguage;
import eu.openanalytics.phaedra.calculationservice.model.CalculationContext;
import eu.openanalytics.phaedra.calculationservice.util.CalculationInputHelper;
import eu.openanalytics.phaedra.measurementservice.client.MeasurementServiceClient;
import eu.openanalytics.phaedra.measurementservice.client.exception.MeasUnresolvableException;
import eu.openanalytics.phaedra.protocolservice.dto.FeatureDTO;
import eu.openanalytics.phaedra.resultdataservice.client.ResultDataServiceClient;
import eu.openanalytics.phaedra.resultdataservice.client.exception.ResultDataUnresolvableException;
import eu.openanalytics.phaedra.scriptengine.client.ScriptEngineClient;
import eu.openanalytics.phaedra.scriptengine.client.model.ScriptExecution;

/**
 * Feature execution is a part of the protocol execution procedure.
 * 
 * One feature is calculated by evaluating one formula. This formula
 * may reference data from other features, or from the plate's "raw"
 * measurement columns.
 * 
 * Formula evaluation is offloaded to an appropriate ScriptEngine that
 * supports the language used by the formula.
 */
@Service
public class FeatureExecutorService {

    private final ScriptEngineClient scriptEngineClient;
    private final MeasurementServiceClient measurementServiceClient;
    private final ResultDataServiceClient resultDataServiceClient;

    private final ObjectMapper objectMapper;
    private final Logger logger = LoggerFactory.getLogger(getClass());

    public FeatureExecutorService(ScriptEngineClient scriptEngineClient, MeasurementServiceClient measurementServiceClient, 
    		ResultDataServiceClient resultDataServiceClient, ObjectMapper objectMapper) {
        this.scriptEngineClient = scriptEngineClient;
        this.measurementServiceClient = measurementServiceClient;
        this.resultDataServiceClient = resultDataServiceClient;
        this.objectMapper = objectMapper;
    }

    public Optional<ScriptExecution> executeFeature(CalculationContext ctx, FeatureDTO feature, Integer currentSequence) {
    	var formula = ctx.getProtocolData().formulas.get(feature.getFormulaId());
    	
        try {
            var inputVariables = collectVariablesForFeature(ctx, feature, currentSequence);
            if (inputVariables.isEmpty()) {
                return Optional.empty();
            }
            
            if (formula.getCategory() != FormulaCategory.CALCULATION
                    || formula.getLanguage() != ScriptLanguage.R
                    || formula.getScope() != CalculationScope.WELL) {
                ctx.getErrorCollector().addError("executing feature => unsupported formula", feature, formula);
                return Optional.empty();
            }

            var execution = scriptEngineClient.newScriptExecution(R_FAST_LANE, formula.getFormula(), 
            		objectMapper.writeValueAsString(inputVariables.get())
            );
            scriptEngineClient.execute(execution);

            return Optional.of(execution);
        } catch (JsonProcessingException e) {
            // this error will probably never occur, see: https://stackoverflow.com/q/26716020/1393103 for examples where it does
            ctx.getErrorCollector().addError("executing feature => writing input variables and request", e, feature, formula);
        }
        return Optional.empty();
    }

    private Optional<HashMap<String, Object>> collectVariablesForFeature(CalculationContext ctx, FeatureDTO feature, Integer currentSequence) {
    	var formula = ctx.getProtocolData().formulas.get(feature.getFormulaId());
    	var inputVariables = new HashMap<String, Object>();

        for (var civ : feature.getCivs()) {
            try {
                if (inputVariables.containsKey(civ.getVariableName())) {
                    // the ProtocolService makes sure this cannot happen, but extra check to make sure
                    ctx.getErrorCollector().addError("executing sequence => executing feature => collecting variables for feature => duplicate variable name detected", feature, formula, civ);
                    return Optional.empty();
                }

                if (civ.getSourceFeatureId() != null) {
                    if (currentSequence == 0) {
                        ctx.getErrorCollector().addError("executing sequence => executing feature => collecting variables for feature => retrieving measurement => trying to get feature in sequence 0", feature, formula, civ);
                        return Optional.empty();
                    }
                    logger.info("Collect result data for feature %s from result set %s", civ.getSourceFeatureId(), ctx.getResultSetId());
                    inputVariables.put(civ.getVariableName(), resultDataServiceClient.getResultData(ctx.getResultSetId(), civ.getSourceFeatureId()).getValues());
                } else if (civ.getSourceMeasColName() != null) {
                    logger.info("Collect result data for measurement %s from result set %s", civ.getSourceMeasColName(), ctx.getMeasId());
                    inputVariables.put(civ.getVariableName(), measurementServiceClient.getWellData(ctx.getMeasId(), civ.getSourceMeasColName()));
                } else {
                    // the ProtocolService makes sure this cannot happen, but extra check to make sure
                    ctx.getErrorCollector().addError("executing sequence => executing feature => collecting variables for feature => retrieving measurement => civ has no valid source", feature, formula, civ);
                    return Optional.empty();
                }

            } catch (MeasUnresolvableException | ResultDataUnresolvableException e) {
                ctx.getErrorCollector().addError("executing sequence => executing feature => collecting variables for feature => retrieving measurement", e, feature, formula, civ);
                return Optional.empty();
            }
        }

        // Add commonly used info about the wells
        CalculationInputHelper.addWellInfo(inputVariables, ctx);

        return Optional.of(inputVariables);
    }
}

