package eu.openanalytics.phaedra.calculationservice.execution.progress;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class CalculationStateEvent {

	private CalculationStage stage;
	private CalculationStateEventCode code;
	private Long featureId;

}
