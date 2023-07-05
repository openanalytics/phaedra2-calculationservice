/**
 * Phaedra II
 *
 * Copyright (C) 2016-2023 Open Analytics
 *
 * ===========================================================================
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the Apache License as published by
 * The Apache Software Foundation, either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Apache License for more details.
 *
 * You should have received a copy of the Apache License
 * along with this program.  If not, see <http://www.apache.org/licenses/>
 */
package eu.openanalytics.phaedra.calculationservice.dto;

import java.time.LocalDateTime;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Null;

import eu.openanalytics.phaedra.calculationservice.dto.validation.OnCreate;
import eu.openanalytics.phaedra.calculationservice.dto.validation.OnUpdate;
import eu.openanalytics.phaedra.calculationservice.enumeration.CalculationScope;
import eu.openanalytics.phaedra.calculationservice.enumeration.FormulaCategory;
import eu.openanalytics.phaedra.calculationservice.enumeration.ScriptLanguage;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.NoArgsConstructor;
import lombok.Value;

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
    FormulaCategory category;

    @NotBlank(message = "Formula is mandatory", groups = {OnCreate.class})
    String formula;

    @NotNull(message = "Language is mandatory", groups = {OnCreate.class})
    ScriptLanguage language;

    @NotNull(message = "Scope is mandatory", groups = {OnCreate.class})
    CalculationScope scope;

    Long previousVersionId;

    String versionNumber;

    @Null(groups = {OnCreate.class, OnUpdate.class}, message = "CreatedBy must be null when creating a formula")
    String createdBy;

    @Null(groups = {OnCreate.class, OnUpdate.class}, message = "CreatedOn must be null when creating a formula")
    LocalDateTime createdOn;

    @Null(groups = {OnCreate.class, OnUpdate.class}, message = "UpdatedBy must be null when creating a formula")
    String updatedBy;

    @Null(groups = {OnCreate.class, OnUpdate.class}, message = "UpdatedOn must be null when creating a formula")
    LocalDateTime updatedOn;
}
