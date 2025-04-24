package eu.openanalytics.phaedra.calculationservice.dto;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class AdHocCalculationResponseDTO {

	private List<AdHocResultData> resultData;

	@Data
	@AllArgsConstructor
	@NoArgsConstructor
	@JsonInclude(JsonInclude.Include.NON_NULL)
	public static class AdHocResultData {
		
		private Long plateId;
		private Long measId;
		private String identifier;
		private float[] values;

	}
}
