package eu.openanalytics.phaedra.calculationservice.execution.input.strategy;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import javax.annotation.Priority;

import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.openanalytics.phaedra.calculationservice.execution.CalculationContext;
import eu.openanalytics.phaedra.calculationservice.execution.input.InputGroup;
import eu.openanalytics.phaedra.calculationservice.model.ModelMapper;
import eu.openanalytics.phaedra.measurementservice.client.MeasurementServiceClient;
import eu.openanalytics.phaedra.protocolservice.dto.FeatureDTO;
import eu.openanalytics.phaedra.resultdataservice.client.ResultDataServiceClient;
import eu.openanalytics.phaedra.resultdataservice.dto.ResultDataDTO;
import eu.openanalytics.phaedra.scriptengine.dto.ScriptExecutionOutputDTO;

/**
 * The default input grouping strategy creates one big input group for the whole feature.
 */
@Component
@Priority(10000)
public class DefaultInputGroupingStrategy extends BaseGroupingStrategy {

	public DefaultInputGroupingStrategy(
			MeasurementServiceClient measurementServiceClient, 
			ResultDataServiceClient resultDataServiceClient, 
			ModelMapper modelMapper,
			ObjectMapper objectMapper) {
		super(measurementServiceClient, resultDataServiceClient, modelMapper, objectMapper);
	}
	
	@Override
	public boolean isSuited(CalculationContext ctx, FeatureDTO feature) {
		// Is always suited, but has lowest priority.
		return true;
	}
	
	@Override
	public Set<InputGroup> createGroups(CalculationContext ctx, FeatureDTO feature) {
		return Collections.singleton(createGroup(ctx, feature, ctx.getWells(), 1));
	}

	@Override
	public ResultDataDTO mergeOutput(CalculationContext ctx, FeatureDTO feature, Map<String, ScriptExecutionOutputDTO> outputs) {
		// Assume a single group has been used
		ScriptExecutionOutputDTO output = outputs.values().iterator().next();
		float[] outputValues = parseOutputValues(output);
		return makeResultData(ctx, feature, outputValues, output);
	}
}
