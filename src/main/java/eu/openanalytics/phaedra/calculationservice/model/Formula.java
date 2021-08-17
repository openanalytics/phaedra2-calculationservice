package eu.openanalytics.phaedra.calculationservice.model;

import eu.openanalytics.phaedra.calculationservice.enumeration.CalculationScope;
import eu.openanalytics.phaedra.calculationservice.enumeration.Category;
import eu.openanalytics.phaedra.calculationservice.enumeration.Language;
import lombok.Data;
import org.springframework.data.annotation.Id;

import javax.validation.constraints.NotNull;
import java.time.LocalDateTime;

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
    private LocalDateTime created_on;
    @NotNull
    private String updated_by;
    @NotNull
    private LocalDateTime updated_on;
}
