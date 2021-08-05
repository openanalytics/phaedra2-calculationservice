package eu.openanalytics.phaedra.calculationservice.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import eu.openanalytics.phaedra.calculationservice.enumeration.CalculationScope;
import eu.openanalytics.phaedra.calculationservice.enumeration.Category;
import eu.openanalytics.phaedra.calculationservice.enumeration.Language;
import lombok.Data;
import org.springframework.data.annotation.Id;

import javax.validation.constraints.NotNull;
import java.util.Date;

@Data
@JsonInclude(JsonInclude.Include.NON_NULL)
public class FormulaDTO {
    private Long id;
    private String name;
    private String description;
    private Category category;
    private String formula;
    private Language language;
    private CalculationScope scope;
    private String created_by;
    private Date created_on;
    private String updated_by;
    private Date updated_on;
}
