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
package eu.openanalytics.phaedra.calculationservice.model;

import java.time.LocalDateTime;

import javax.validation.constraints.NotNull;

import org.springframework.data.annotation.Id;

import eu.openanalytics.phaedra.calculationservice.enumeration.CalculationScope;
import eu.openanalytics.phaedra.calculationservice.enumeration.FormulaCategory;
import eu.openanalytics.phaedra.calculationservice.enumeration.ScriptLanguage;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Value;
import lombok.With;

@Value
@With
@Builder(toBuilder = true)
@AllArgsConstructor
public class Formula {
	
    @Id
    Long id;

    @NotNull
    String name;

    @NotNull
    String description;

    @NotNull
    FormulaCategory category;

    @NotNull
    String formula;

    @NotNull
    ScriptLanguage language;

    @NotNull
    CalculationScope scope;

    Long previousVersionId;

    @NotNull
    String versionNumber;
    
    boolean deprecated;

    @NotNull
    String createdBy;

    @NotNull
    LocalDateTime createdOn;

    String updatedBy;

    LocalDateTime updatedOn;
}
