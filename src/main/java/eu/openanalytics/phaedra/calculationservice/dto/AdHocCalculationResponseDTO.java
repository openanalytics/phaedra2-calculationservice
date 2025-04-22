package eu.openanalytics.phaedra.calculationservice.dto;

import java.util.Map;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class AdHocCalculationResponseDTO {

	private Map<Long, float[]> values;

}
