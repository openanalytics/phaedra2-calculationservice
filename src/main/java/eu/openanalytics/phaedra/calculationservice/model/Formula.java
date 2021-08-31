package eu.openanalytics.phaedra.calculationservice.model;

import eu.openanalytics.phaedra.calculationservice.enumeration.CalculationScope;
import eu.openanalytics.phaedra.calculationservice.enumeration.Category;
import eu.openanalytics.phaedra.calculationservice.enumeration.ScriptLanguage;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Value;
import lombok.With;
import org.springframework.data.annotation.Id;

import javax.validation.constraints.NotNull;
import java.time.LocalDateTime;

//@Data
@Value
@With
@Builder
@AllArgsConstructor
public class Formula {
    @Id
    Long id;

    @NotNull
    String name;

    @NotNull
    String description;

    @NotNull
    Category category;

    @NotNull
    String formula;

    @NotNull
    ScriptLanguage language;

    @NotNull
    CalculationScope scope;

    @NotNull
    String createdBy;

    @NotNull
    LocalDateTime createdOn;

    String updatedBy;

    LocalDateTime updatedOn;
}
