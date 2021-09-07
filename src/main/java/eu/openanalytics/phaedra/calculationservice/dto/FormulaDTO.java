package eu.openanalytics.phaedra.calculationservice.dto;

import eu.openanalytics.phaedra.calculationservice.dto.validation.OnCreate;
import eu.openanalytics.phaedra.calculationservice.dto.validation.OnUpdate;
import eu.openanalytics.phaedra.calculationservice.enumeration.CalculationScope;
import eu.openanalytics.phaedra.calculationservice.enumeration.Category;
import eu.openanalytics.phaedra.calculationservice.enumeration.ScriptLanguage;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.NoArgsConstructor;
import lombok.Value;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Null;
import java.time.LocalDateTime;

@Value
@Builder
@AllArgsConstructor
@NoArgsConstructor(force = true, access = AccessLevel.PRIVATE) // Jackson deserialize compatibility
public class FormulaDTO {

    @Null(groups = OnCreate.class, message = "Id must be null when creating a formula")
    @Null(groups = OnUpdate.class, message = "Id is mandatory when updating a formula")
    Long id;

    @NotBlank(message = "Name is mandatory", groups = {OnCreate.class})
    String name;

    String description;

    @NotNull(message = "Category is mandatory", groups = {OnCreate.class})
    Category category;

    @NotBlank(message = "Formula is mandatory", groups = {OnCreate.class})
    String formula;

    @NotNull(message = "Language is mandatory", groups = {OnCreate.class})
    ScriptLanguage language;

    @NotNull(message = "Scope is mandatory", groups = {OnCreate.class})
    CalculationScope scope;

    @Null(groups = {OnCreate.class, OnUpdate.class}, message = "CreatedBy must be null when creating a formula")
    String createdBy;

    @Null(groups = {OnCreate.class, OnUpdate.class}, message = "CreatedOn must be null when creating a formula")
    LocalDateTime createdOn;

    @Null(groups = {OnCreate.class, OnUpdate.class}, message = "UpdatedBy must be null when creating a formula")
    String updatedBy;

    @Null(groups = {OnCreate.class, OnUpdate.class}, message = "UpdatedOn must be null when creating a formula")
    LocalDateTime updatedOn;
}
