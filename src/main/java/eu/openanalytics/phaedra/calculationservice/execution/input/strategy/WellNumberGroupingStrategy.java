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

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import jakarta.annotation.Priority;

import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.openanalytics.phaedra.calculationservice.execution.CalculationContext;
import eu.openanalytics.phaedra.calculationservice.execution.input.InputGroup;
import eu.openanalytics.phaedra.calculationservice.model.ModelMapper;
import eu.openanalytics.phaedra.measurementservice.client.MeasurementServiceClient;
import eu.openanalytics.phaedra.protocolservice.dto.FeatureDTO;
import eu.openanalytics.phaedra.protocolservice.enumeration.InputSource;
import eu.openanalytics.phaedra.resultdataservice.client.ResultDataServiceClient;
import eu.openanalytics.phaedra.resultdataservice.dto.ResultDataDTO;
import eu.openanalytics.phaedra.scriptengine.dto.ScriptExecutionOutputDTO;

@Component
@Priority(1)
public class WellNumberGroupingStrategy extends BaseGroupingStrategy {

	private Environment environment;

	public WellNumberGroupingStrategy(Environment environment,
			MeasurementServiceClient measurementServiceClient, ResultDataServiceClient resultDataServiceClient,
			ModelMapper modelMapper, ObjectMapper objectMapper) {
		super(measurementServiceClient, resultDataServiceClient, modelMapper, objectMapper);
		this.environment = environment;
	}

	@Override
	public boolean isSuited(CalculationContext ctx, FeatureDTO feature) {
		// This strategy is suited for large subwell-data based calculations.
		if (feature.getCivs() == null || feature.getCivs().isEmpty()) return false;
		return feature.getCivs().stream().anyMatch(civ -> civ.getInputSource() == InputSource.MEASUREMENT_SUBWELL_COLUMN);
	}

	@Override
	public Set<InputGroup> createGroups(CalculationContext ctx, FeatureDTO feature) {
		int groupSize = Integer.valueOf(environment.getProperty("phaedra.calculation.input.group.size", "10"));
		return ctx.getWells().stream()
				.collect(Collectors.groupingBy(well -> (well.getWellNr())/groupSize))
				.entrySet().stream().map(entry -> createGroup(ctx, feature, entry.getValue(), entry.getKey()))
				.collect(Collectors.toSet());
	}

	@Override
	public ResultDataDTO mergeOutput(CalculationContext ctx, FeatureDTO feature, Map<String, ScriptExecutionOutputDTO> outputs) {
		List<ScriptExecutionOutputDTO> sortedOutputs = outputs.entrySet().stream()
				.sorted((e1, e2) -> Integer.parseInt(e1.getKey()) - Integer.parseInt(e2.getKey()))
				.map(e -> e.getValue()).toList();
		List<float[]> allValues = sortedOutputs.stream().map(output -> parseOutputValues(output)).toList();

		int totalSize = allValues.stream().mapToInt(v -> v.length).sum();
		float[] mergedValues = new float[totalSize];
		int pos = 0;
		for (float[] v: allValues) {
			System.arraycopy(v, 0, mergedValues, pos, v.length);
			pos += v.length;
		}

		return makeResultData(ctx, feature, mergedValues, sortedOutputs.get(0));
	}
}
