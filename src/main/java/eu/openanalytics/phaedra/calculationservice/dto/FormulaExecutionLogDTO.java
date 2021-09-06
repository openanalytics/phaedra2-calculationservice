package eu.openanalytics.phaedra.calculationservice.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Data;

import java.util.Date;

@Data
@JsonInclude(JsonInclude.Include.NON_NULL)
public class FormulaExecutionLogDTO {
    private Long id;
    private Long formula_id;
    private Long feature_id;
    private String executed_by;
    private Date executed_on = new Date();
}
