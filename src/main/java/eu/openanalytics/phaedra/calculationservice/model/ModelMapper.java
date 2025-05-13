/**
 * Phaedra II
 *
 * Copyright (C) 2016-2025 Open Analytics
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

import eu.openanalytics.phaedra.calculationservice.enumeration.ResponseStatusCode;
import org.modelmapper.Conditions;
import org.modelmapper.config.Configuration;
import org.modelmapper.convention.NameTransformers;
import org.modelmapper.convention.NamingConventions;
import org.springframework.stereotype.Service;

import eu.openanalytics.phaedra.calculationservice.dto.FormulaDTO;
import eu.openanalytics.phaedra.calculationservice.enumeration.CalculationStatusCode;
import eu.openanalytics.phaedra.resultdataservice.enumeration.StatusCode;

@Service
public class ModelMapper {

    private final org.modelmapper.ModelMapper modelMapper = new org.modelmapper.ModelMapper();

    public ModelMapper() {
        Configuration builderConfiguration = modelMapper.getConfiguration().copy()
                .setDestinationNameTransformer(NameTransformers.builder())
                .setDestinationNamingConvention(NamingConventions.builder());

        modelMapper.createTypeMap(FormulaDTO.class, Formula.FormulaBuilder.class, builderConfiguration)
                .setPropertyCondition(Conditions.isNotNull());

        modelMapper.createTypeMap(Formula.class, FormulaDTO.FormulaDTOBuilder.class, builderConfiguration)
                .setPropertyCondition(Conditions.isNotNull());

        modelMapper.validate(); // ensure that objects can be mapped
    }

    /**
     * Returns a Builder that contains the properties of {@link Formula}, which are updated with the
     * values of a {@link FormulaDTO} while ignore properties in the {@link FormulaDTO} that are null.
     * The return value can be further customized by calling the builder methods.
     * This function should be used for PUT requests.
     */
    public Formula.FormulaBuilder map(FormulaDTO formulaDTO, Formula formula) {
        Formula.FormulaBuilder builder = formula.toBuilder();
        modelMapper.map(formulaDTO, builder);
        return builder;
    }

    /**
     * Maps a {@link FormulaDTO} to a {@link Formula.FormulaBuilder}.
     * The return value can be further customized by calling the builder methods.
     */
    public Formula.FormulaBuilder map(FormulaDTO formulaDTO) {
        Formula.FormulaBuilder builder = Formula.builder();
        modelMapper.map(formulaDTO, builder);
        return builder;
    }

    /**
     * Maps a {@link Formula} to a {@link FormulaDTO.FormulaDTOBuilder}.
     * The return value can be further customized by calling the builder methods.
     */
    public FormulaDTO.FormulaDTOBuilder map(Formula formula) {
        FormulaDTO.FormulaDTOBuilder builder = FormulaDTO.builder();
        modelMapper.map(formula, builder);
        return builder;
    }

    /**
     * Maps a {@link ResponseStatusCode} to a {@link StatusCode}.
     */
    public StatusCode map(ResponseStatusCode responseStatusCode) {
        return switch (responseStatusCode) {
            case SUCCESS -> StatusCode.SUCCESS;
            case SCRIPT_ERROR, BAD_REQUEST, WORKER_INTERNAL_ERROR, RESCHEDULED_BY_WATCHDOG  -> StatusCode.FAILURE;
        };
    }

    public CalculationStatusCode map(StatusCode statusCode) {
        return switch (statusCode) {
            case SUCCESS -> CalculationStatusCode.SUCCESS;
            case FAILURE -> CalculationStatusCode.FAILURE;
            case SCHEDULED -> CalculationStatusCode.SCHEDULED;
        };
    }

}
