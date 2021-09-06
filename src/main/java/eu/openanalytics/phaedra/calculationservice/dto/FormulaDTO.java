package eu.openanalytics.phaedra.calculationservice.dto;

import eu.openanalytics.phaedra.calculationservice.dto.validation.OnCreate;
import eu.openanalytics.phaedra.calculationservice.dto.validation.OnUpdate;
import eu.openanalytics.phaedra.calculationservice.enumeration.CalculationScope;
import eu.openanalytics.phaedra.calculationservice.enumeration.Category;
import eu.openanalytics.phaedra.calculationservice.enumeration.ScriptLanguage;
import lombok.Data;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Null;
import java.time.LocalDateTime;

@Data
public class FormulaDTO {

    @Null(groups = OnCreate.class, message = "Id must be null when creating a formula")
    @NotNull(groups = OnUpdate.class, message = "Id is mandatory when updating a formula")
    private Long id;

    @NotBlank(message = "Name is mandatory", groups = {OnCreate.class})
    private String name;

    private String description;

    @NotNull(message = "Category is mandatory", groups = {OnCreate.class})
    private Category category;

    @NotBlank(message = "Formula is mandatory", groups = {OnCreate.class})
    private String formula;

    @NotNull(message = "Language is mandatory", groups = {OnCreate.class})
    private ScriptLanguage language;

    @NotNull(message = "Scope is mandatory", groups = {OnCreate.class})
    private CalculationScope scope;

    @Null(groups = {OnCreate.class, OnUpdate.class}, message = "CreatedBy must be null when creating a formula")
    private String createdBy;

    @Null(groups = {OnCreate.class, OnUpdate.class}, message = "CreatedOn must be null when creating a formula")
    private LocalDateTime createdOn;

    @Null(groups = {OnCreate.class, OnUpdate.class}, message = "UpdatedBy must be null when creating a formula")
    private String updatedBy;

    @Null(groups = {OnCreate.class, OnUpdate.class}, message = "UpdatedOn must be null when creating a formula")
    private LocalDateTime updatedOn;
}
