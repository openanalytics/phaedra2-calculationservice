package eu.openanalytics.phaedra.calculationservice.model;

import lombok.Data;
import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Table;

import javax.validation.constraints.NotNull;
import java.util.Date;

@Table("formula_execution_log")
@Data
public class FormulaExecutionLog {
    @Id
    private Long id;
    @NotNull
    private Long formula_id;
    @NotNull
    private Long feature_id;
    @NotNull
    private String executed_by;
    @NotNull
    private Date executed_on = new Date();
}
