package eu.openanalytics.phaedra.calculationservice.model;

import eu.openanalytics.phaedra.calculationservice.enumeration.CalculationScope;
import eu.openanalytics.phaedra.calculationservice.enumeration.Category;
import eu.openanalytics.phaedra.calculationservice.enumeration.Language;
import lombok.Data;
import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Table;

import javax.inject.Scope;
import javax.validation.constraints.NotNull;
import java.util.Date;

@Data
public class Formula {
    @Id
    private Long id;
    @NotNull
    private String name;
    @NotNull
    private String description;
    @NotNull
    private Category category;
    @NotNull
    private String formula;
    @NotNull
    private Language language;
    @NotNull
    private CalculationScope scope;
    @NotNull
    private String created_by;
    @NotNull
    private Date created_on = new Date();
    @NotNull
    private String updated_by;
    @NotNull
    private Date updated_on;
}
