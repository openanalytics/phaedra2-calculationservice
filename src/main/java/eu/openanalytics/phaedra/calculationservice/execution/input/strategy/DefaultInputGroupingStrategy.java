/**
 * Phaedra II
 *
 * Copyright (C) 2016-2024 Open Analytics
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
