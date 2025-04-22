package eu.openanalytics.phaedra.calculationservice.dto;

import java.util.List;
import java.util.Map;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class AdHocCalculationRequestDTO {

    private List<Long> plateIds;
    private Map<Long, Long> measIds;

    private Long formulaId;
    private Map<String, String> civs;

}
