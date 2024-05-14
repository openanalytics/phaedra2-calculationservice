package eu.openanalytics.phaedra.calculationservice.execution.input;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import eu.openanalytics.phaedra.calculationservice.exception.CalculationException;
import eu.openanalytics.phaedra.calculationservice.execution.CalculationContext;
import eu.openanalytics.phaedra.calculationservice.model.Formula;
import eu.openanalytics.phaedra.calculationservice.model.ModelMapper;
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
 * The default input grouping strategy creates one big input group for the whole feature.
 */
public class DefaultInputGroupingStrategy implements InputGroupingStrategy {

	private MeasurementServiceClient measurementServiceClient;
	private ResultDataServiceClient resultDataServiceClient;
	private final ModelMapper modelMapper;
	private final ObjectMapper objectMapper;
	
	public DefaultInputGroupingStrategy(
			MeasurementServiceClient measurementServiceClient, 
			ResultDataServiceClient resultDataServiceClient, 
			ModelMapper modelMapper,
			ObjectMapper objectMapper) {
		this.measurementServiceClient = measurementServiceClient;
		this.resultDataServiceClient = resultDataServiceClient;
		this.modelMapper = modelMapper;
		this.objectMapper = objectMapper;
	}
	
	@Override
	public Set<InputGroup> createGroups(CalculationContext ctx, FeatureDTO feature) {
		return Collections.singleton(createSingleGroup(ctx, feature));
	}

	@Override
	public ResultDataDTO mergeOutput(CalculationContext ctx, FeatureDTO feature, Set<ScriptExecutionOutputDTO> outputs) {
		// Assume a single group has been used
		ScriptExecutionOutputDTO output = outputs.iterator().next();
		float[] outputValues = parseOutputValues(output);
		
		ResultDataDTO resultData = ResultDataDTO.builder()
		        .resultSetId(ctx.getResultSetId())
		        .featureId(feature.getId())
		        .values(outputValues)
		        .statusCode(modelMapper.map(output.getStatusCode()))
		        .statusMessage(output.getStatusMessage())
		        .exitCode(output.getExitCode())
		        .build();
		
		return resultData;
	}

	private InputGroup createSingleGroup(CalculationContext ctx, FeatureDTO feature) {
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
        
        InputGroup group = new InputGroup();
        group.setGroupNumber(1);
        group.setInputVariables(inputVariables);
        return group;
	}
	
	private float[] parseOutputValues(ScriptExecutionOutputDTO output) {
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
