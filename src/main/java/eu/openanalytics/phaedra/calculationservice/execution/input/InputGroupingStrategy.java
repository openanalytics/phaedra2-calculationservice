package eu.openanalytics.phaedra.calculationservice.execution.input;

import java.util.Set;

import eu.openanalytics.phaedra.calculationservice.execution.CalculationContext;
import eu.openanalytics.phaedra.protocolservice.dto.FeatureDTO;
import eu.openanalytics.phaedra.resultdataservice.dto.ResultDataDTO;
import eu.openanalytics.phaedra.scriptengine.dto.ScriptExecutionOutputDTO;

/**
 * A strategy for splitting input data into groups when evaluating a Feature formula.
 * Each group will then be handled in a separate (parallel) script execution.
 * 
 * This is most useful for subwell-data calculation, where the full input size may present
 * performance or scalability issues.
 */
public interface InputGroupingStrategy {

	public Set<InputGroup> createGroups(CalculationContext ctx, FeatureDTO feature);

	public ResultDataDTO mergeOutput(CalculationContext ctx, FeatureDTO feature, Set<ScriptExecutionOutputDTO> outputs);

}
