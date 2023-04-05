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

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;

import org.springframework.stereotype.Service;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import eu.openanalytics.phaedra.calculationservice.dto.CurveFittingRequestDTO;
import eu.openanalytics.phaedra.calculationservice.enumeration.CalculationScope;
import eu.openanalytics.phaedra.calculationservice.exception.CalculationException;
import eu.openanalytics.phaedra.calculationservice.model.CalculationContext;
import eu.openanalytics.phaedra.calculationservice.model.Formula;
import eu.openanalytics.phaedra.calculationservice.model.ModelMapper;
import eu.openanalytics.phaedra.calculationservice.service.KafkaProducerService;
import eu.openanalytics.phaedra.calculationservice.service.script.ScriptExecutionRequest;
import eu.openanalytics.phaedra.calculationservice.service.script.ScriptExecutionService;
import eu.openanalytics.phaedra.calculationservice.util.CalculationInputHelper;
import eu.openanalytics.phaedra.measurementservice.client.MeasurementServiceClient;
import eu.openanalytics.phaedra.measurementservice.client.exception.MeasUnresolvableException;
import eu.openanalytics.phaedra.protocolservice.dto.CalculationInputValueDTO;
import eu.openanalytics.phaedra.protocolservice.dto.FeatureDTO;
import eu.openanalytics.phaedra.resultdataservice.client.ResultDataServiceClient;
import eu.openanalytics.phaedra.resultdataservice.client.exception.ResultDataUnresolvableException;
import eu.openanalytics.phaedra.resultdataservice.dto.ResultDataDTO;
import eu.openanalytics.phaedra.scriptengine.dto.ResponseStatusCode;
import eu.openanalytics.phaedra.scriptengine.dto.ScriptExecutionOutputDTO;

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

    private final MeasurementServiceClient measurementServiceClient;
    private final ResultDataServiceClient resultDataServiceClient;
    
    private final FeatureStatExecutorService featureStatExecutorService;
    private final ScriptExecutionService scriptExecutionService;
    private final KafkaProducerService kafkaProducerService;
    
    private final ObjectMapper objectMapper;
    private final ModelMapper modelMapper;

    public FeatureExecutorService(
    		MeasurementServiceClient measurementServiceClient, 
    		ResultDataServiceClient resultDataServiceClient,
    		FeatureStatExecutorService featureStatExecutorService,
    		ScriptExecutionService scriptExecutionService,
    		KafkaProducerService kafkaProducerService,
    		ModelMapper modelMapper, ObjectMapper objectMapper) {
    	
        this.measurementServiceClient = measurementServiceClient;
        this.resultDataServiceClient = resultDataServiceClient;
        this.featureStatExecutorService = featureStatExecutorService;
        this.scriptExecutionService = scriptExecutionService;
        this.kafkaProducerService = kafkaProducerService;
        this.objectMapper = objectMapper;
        this.modelMapper = modelMapper;
    }

    /**
     * Calculate the value for a feature.
     * This operation does not block, and will return as soon as the request is launched.
     */
    public ScriptExecutionRequest executeFeature(CalculationContext ctx, FeatureDTO feature, Integer currentSequence) {
    	
    	// Retrieve and validate the formula
    	Formula formula = ctx.getProtocolData().formulas.get(feature.getFormulaId());
    	if (formula.getScope() != CalculationScope.WELL) {
    		ctx.getErrorCollector().addError("Invalid formula scope for feature calculation", feature, formula);
    		return null;
    	}
    	
    	// Collect all required input data
    	Map<String, Object> inputVariables = null;
    	try {
    		inputVariables = collectInputVariables(ctx, feature, currentSequence);
    	} catch (CalculationException e) {
    		// Appropriate errors have already been added to the ErrorCollector.
    		return null;
    	}
    	
    	// Submit the calculation request
    	ScriptExecutionRequest request = scriptExecutionService
			.submit(formula.getLanguage(), formula.getFormula(), inputVariables)
			.addCallback(output -> {
	    		float[] outputValues = parseNumericValues(output);
	    		
	    		// Publish the result data
	    		ResultDataDTO resultData = ResultDataDTO.builder()
	    		        .resultSetId(ctx.getResultSetId())
	    		        .featureId(feature.getId())
	    		        .values(outputValues)
	    		        .statusCode(modelMapper.map(output.getStatusCode()))
	    		        .statusMessage(output.getStatusMessage())
	    		        .exitCode(output.getExitCode())
	    		        .build();
	    		kafkaProducerService.sendResultData(resultData);
	    		
	    		if (output.getStatusCode() == ResponseStatusCode.SUCCESS) {
	    			// Submit feature stats calculation
	    			featureStatExecutorService.executeFeatureStats(ctx, feature, outputValues);
	    			
	    			// Submit curve fitting request
	    			var curveFitRequest = new CurveFittingRequestDTO(ctx.getPlate().getId(), resultData.getFeatureId(), resultData);
	    			kafkaProducerService.initiateCurveFitting(curveFitRequest);
	            } else {
	            	ctx.getErrorCollector().addError(String.format("Script execution failed with status %s", output.getStatusCode()), output, feature, formula);
	            	ctx.getCalculationProgress().updateProgressFeature(feature.getId(), false);
	            }
    	});
    	return request;
    }

    private Map<String, Object> collectInputVariables(CalculationContext ctx, FeatureDTO feature, Integer currentSequence) {
    	Map<String, Object> inputVariables = new HashMap<String, Object>();
    	Formula formula = ctx.getProtocolData().formulas.get(feature.getFormulaId());

    	BiConsumer<String, CalculationInputValueDTO> errorHandler = (msg, civ) -> {
    		ctx.getErrorCollector().addError(msg, feature, formula, civ);
    		throw new CalculationException(msg);
    	};
    	
        for (var civ : feature.getCivs()) {
            if (inputVariables.containsKey(civ.getVariableName())) {
            	errorHandler.accept("Duplicate variable name", civ);
            }
            switch (civ.getInputSource()) {
            case FEATURE:
            	if (currentSequence == 0) {
            		errorHandler.accept("Cannot reference another feature in sequence 0", civ);
                } else if (civ.getSourceFeatureId() == null) {
                	errorHandler.accept("Feature reference is missing ID", civ);
            	} else {
            		try {
						inputVariables.put(civ.getVariableName(), resultDataServiceClient.getResultData(ctx.getResultSetId(), civ.getSourceFeatureId()).getValues());
					} catch (ResultDataUnresolvableException e) {
						errorHandler.accept("Failed to retrieve feature source data", civ);
					}
            	}
                break;
            case MEASUREMENT_WELL_COLUMN:
            	if (civ.getSourceMeasColName() == null || civ.getSourceMeasColName().trim().isEmpty()) {
            		errorHandler.accept("Measurement reference is missing column name", civ);
            	} else {
            		try {
            			inputVariables.put(civ.getVariableName(), measurementServiceClient.getWellData(ctx.getMeasId(), civ.getSourceMeasColName()));
            		} catch (MeasUnresolvableException e) {
            			errorHandler.accept("Failed to retrieve measurement source welldata", civ);
            		}
            	}
                break;
            case MEASUREMENT_SUBWELL_COLUMN:
            	if (civ.getSourceMeasColName() == null || civ.getSourceMeasColName().trim().isEmpty()) {
            		errorHandler.accept("Measurement reference is missing column name", civ);
            	} else {
            		try {
            			inputVariables.put(civ.getVariableName(), measurementServiceClient.getSubWellData(ctx.getMeasId(), civ.getSourceMeasColName()));
            		} catch (MeasUnresolvableException e) {
            			errorHandler.accept("Failed to retrieve measurement source subwelldata", civ);
            		}            		
            	}
                break;
            default:
            	errorHandler.accept("Invalid variable source reference", civ);
            }
        }

        // Add commonly used info about the wells
        CalculationInputHelper.addWellInfo(inputVariables, ctx);
        
        return inputVariables;
    }
    
    private float[] parseNumericValues(ScriptExecutionOutputDTO output) {
    	if (output.getOutput() == null || output.getStatusCode() != ResponseStatusCode.SUCCESS) return null;
    	
    	String[] outputStrings = null;
    	try {
    		OutputWrapper outputValue = objectMapper.readValue(output.getOutput(), OutputWrapper.class);
    		outputStrings = outputValue.output;
    	} catch (JsonProcessingException e) {
    		return null;
    	}
    	if (outputStrings == null) return null;
    	
    	float[] numericOutput = new float[outputStrings.length];
        for (int i = 0; i < numericOutput.length; i++) {
            try {
            	numericOutput[i] = Float.parseFloat(outputStrings[i]);
            } catch (Exception e) {
            	numericOutput[i] = Float.NaN;
            }
        }
        return numericOutput;
    }
    
    private static class OutputWrapper {

        public final String[] output;

        @JsonCreator
        private OutputWrapper(@JsonProperty(value = "output", required = true) String[] output) {
            this.output = output;
        }
    }
}

