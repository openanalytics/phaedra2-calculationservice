/**
 * Phaedra II
 *
 * Copyright (C) 2016-2025 Open Analytics
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
package eu.openanalytics.phaedra.calculationservice.execution.input.strategy;

import eu.openanalytics.phaedra.calculationservice.dto.ScriptExecutionOutputDTO;
import eu.openanalytics.phaedra.calculationservice.enumeration.ResponseStatusCode;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import eu.openanalytics.phaedra.calculationservice.exception.CalculationException;
import eu.openanalytics.phaedra.calculationservice.execution.CalculationContext;
import eu.openanalytics.phaedra.calculationservice.execution.input.CalculationInputHelper;
import eu.openanalytics.phaedra.calculationservice.execution.input.InputGroup;
import eu.openanalytics.phaedra.calculationservice.model.Formula;
import eu.openanalytics.phaedra.calculationservice.model.ModelMapper;
import eu.openanalytics.phaedra.measurementservice.client.MeasurementServiceClient;
import eu.openanalytics.phaedra.measurementservice.client.exception.MeasUnresolvableException;
import eu.openanalytics.phaedra.plateservice.dto.WellDTO;
import eu.openanalytics.phaedra.protocolservice.dto.CalculationInputValueDTO;
import eu.openanalytics.phaedra.protocolservice.dto.FeatureDTO;
import eu.openanalytics.phaedra.resultdataservice.client.ResultDataServiceClient;
import eu.openanalytics.phaedra.resultdataservice.client.exception.ResultDataUnresolvableException;
import eu.openanalytics.phaedra.resultdataservice.dto.ResultDataDTO;

public abstract class BaseGroupingStrategy implements InputGroupingStrategy {

	private final MeasurementServiceClient measurementServiceClient;
	private final ResultDataServiceClient resultDataServiceClient;

	private final ModelMapper modelMapper;
	private final ObjectMapper objectMapper;

	public BaseGroupingStrategy(MeasurementServiceClient measurementServiceClient, ResultDataServiceClient resultDataServiceClient, ModelMapper modelMapper, ObjectMapper objectMapper) {
		this.measurementServiceClient = measurementServiceClient;
		this.resultDataServiceClient = resultDataServiceClient;
		this.modelMapper = modelMapper;
		this.objectMapper = objectMapper;
	}

	protected InputGroup createGroup(CalculationContext ctx, FeatureDTO feature, List<WellDTO> wells, int groupNr) {
		// Note: assuming here that the list of wells is consecutive (i.e. no gaps)
		int[] wellNrRange = {
				wells.stream().mapToInt(well -> well.getWellNr()).min().orElse(1),
				wells.stream().mapToInt(well -> well.getWellNr()).max().orElse(1)
		};

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
            	if (ctx.getStateTracker().getCurrentSequence() == 0) {
            		errorHandler.accept("Cannot reference another feature in sequence 0", civ);
                } else if (civ.getSourceFeatureId() == null) {
                	errorHandler.accept("Feature reference is missing ID", civ);
            	} else {
            		try {
            			ResultDataDTO resultData = resultDataServiceClient.getResultData(ctx.getResultSetId(), civ.getSourceFeatureId());
            			float[] values = Arrays.copyOfRange(resultData.getValues(), wellNrRange[0] - 1, wellNrRange[1]);
						inputVariables.put(civ.getVariableName(), values);
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
            			float[] values = measurementServiceClient.getWellData(ctx.getMeasId(), civ.getSourceMeasColName());
            			values = Arrays.copyOfRange(values, wellNrRange[0] - 1, wellNrRange[1]);
            			inputVariables.put(civ.getVariableName(), values);
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
            			if (!ctx.getSubwellDataCache().containsKey(civ.getSourceMeasColName())) {
            				ctx.getSubwellDataCache().put(civ.getSourceMeasColName(), measurementServiceClient.getSubWellData(ctx.getMeasId(), civ.getSourceMeasColName()));
            			}
            			Map<Integer, float[]> values = ctx.getSubwellDataCache().get(civ.getSourceMeasColName());
            			if (values == null || values.isEmpty()) errorHandler.accept("No measurement subwelldata available for " + civ.getSourceMeasColName(), civ);

            			float[][] valueArrays = new float[1 + (wellNrRange[1] - wellNrRange[0])][];
            			for (int nr = wellNrRange[0]; nr <= wellNrRange[1]; nr++) {
            				valueArrays[nr - wellNrRange[0]] = values.get(nr);
            			}
            			inputVariables.put(civ.getVariableName(), valueArrays);
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
        CalculationInputHelper.addWellInfo(inputVariables, ctx, wells);

        InputGroup group = new InputGroup();
        group.setGroupNumber(groupNr);
        group.setInputVariables(inputVariables);
        return group;
	}

	protected ResultDataDTO makeResultData(CalculationContext ctx, FeatureDTO feature, float[] values, ScriptExecutionOutputDTO statusOutput) {
		return ResultDataDTO.builder()
		        .resultSetId(ctx.getResultSetId())
		        .featureId(feature.getId())
		        .values(values)
		        .statusCode(modelMapper.map(statusOutput.getStatusCode()))
		        .statusMessage(statusOutput.getStatusMessage())
		        .build();
	}

	protected float[] parseOutputValues(ScriptExecutionOutputDTO output) {
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
