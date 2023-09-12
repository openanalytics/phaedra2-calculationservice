package eu.openanalytics.phaedra.calculationservice.dto.event;

import com.fasterxml.jackson.annotation.JsonInclude;

import eu.openanalytics.phaedra.plateservice.enumeration.CalculationStatus;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@JsonInclude(JsonInclude.Include.NON_NULL)
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class CalculationEvent {

	private Long plateId;
	private Long measurementId;
	private Long protocolId;
	
	private CalculationStatus calculationStatus;
}
