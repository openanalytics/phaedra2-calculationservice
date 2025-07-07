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
import eu.openanalytics.phaedra.calculationservice.execution.CalculationContext;
import eu.openanalytics.phaedra.calculationservice.execution.input.InputGroup;
import eu.openanalytics.phaedra.protocolservice.dto.FeatureDTO;
import eu.openanalytics.phaedra.resultdataservice.dto.ResultDataDTO;
import java.util.Map;
import java.util.Set;

/**
 * A strategy for splitting input data into groups when evaluating a Feature formula.
 * Each group will then be handled in a separate (parallel) script execution.
 *
 * This is most useful for subwell-data calculation, where the full input size may present
 * performance or scalability issues.
 */
public interface InputGroupingStrategy {

	/* package */ boolean isSuited(CalculationContext ctx, FeatureDTO feature);

	public Set<InputGroup> createGroups(CalculationContext ctx, FeatureDTO feature);

	public ResultDataDTO mergeOutput(CalculationContext ctx, FeatureDTO feature, Map<String, ScriptExecutionOutputDTO> outputs);

}
