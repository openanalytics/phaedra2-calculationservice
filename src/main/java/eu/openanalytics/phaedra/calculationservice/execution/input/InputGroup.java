package eu.openanalytics.phaedra.calculationservice.execution.input;

import java.util.Map;

import lombok.Data;

@Data
public class InputGroup {

	private int groupNumber;
	private Map<String, Object> inputVariables;

}
