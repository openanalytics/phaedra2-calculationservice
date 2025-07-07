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
package eu.openanalytics.phaedra.calculationservice.util;

import static eu.openanalytics.phaedra.calculationservice.util.LoggerHelper.log;

import eu.openanalytics.phaedra.calculationservice.dto.ScriptExecutionOutputDTO;
import eu.openanalytics.phaedra.calculationservice.execution.CalculationContext;
import eu.openanalytics.phaedra.calculationservice.model.Formula;
import eu.openanalytics.phaedra.protocolservice.dto.CalculationInputValueDTO;
import eu.openanalytics.phaedra.protocolservice.dto.FeatureDTO;
import eu.openanalytics.phaedra.protocolservice.dto.FeatureStatDTO;
import eu.openanalytics.phaedra.resultdataservice.dto.ErrorDTO;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ErrorCollector {

    private final List<ErrorDTO> errors = Collections.synchronizedList(new ArrayList<>());
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final CalculationContext ctx;

    public ErrorCollector(CalculationContext ctx) {
        this.ctx = ctx;
    }

    public List<ErrorDTO> getErrors() {
        return errors;
    }

    public String getErrorDescription() {
        StringBuilder description = new StringBuilder();
        for (var error : errors) {
            description.append(error);
            description.append("\n");
        }
        return description.toString();
    }

    public boolean hasError() {
        return errors.size() > 0;
    }

    public void addError(String description, Object... ctxObjects) {
        var errorBuilder = ErrorDTO.builder()
                .timestamp(LocalDateTime.now())
                .description(description);

        Optional<Throwable> exception = Optional.empty();

        for (Object ctxObject : ctxObjects) {
            if (ctxObject instanceof FeatureDTO feature) {
                errorBuilder
                        .sequenceNumber(feature.getSequence())
                        .featureId(feature.getId())
                        .featureName(feature.getName());
            } else if (ctxObject instanceof ScriptExecutionOutputDTO output) {
                errorBuilder
                        .statusMessage(output.getStatusMessage());
            } else if (ctxObject instanceof CalculationInputValueDTO civ) {
                errorBuilder
                        .civType(civ.getInputSource().name())
                        .civVariableName(civ.getVariableName())
                        .civSource(civ.getSourceMeasColName() == null ? String.valueOf(civ.getSourceFeatureId()) : civ.getSourceMeasColName());
            } else if (ctxObject instanceof FeatureStatDTO featureStat) {
                errorBuilder
                        .featureStatId(featureStat.getId())
                        .featureStatName(featureStat.getName());
            } else if (ctxObject instanceof Formula formula) {
                errorBuilder
                        .formulaId(formula.getId())
                        .formulaName(formula.getName());
            } else if (ctxObject instanceof Throwable e) {
                errorBuilder
                        .exceptionClassName(e.getClass().getSimpleName())
                        .exceptionMessage(e.getMessage());
                if (exception.isPresent()) {
                    log(logger, ctx,"Multiple exception provided to errorCollector:handleError");
                }
                exception = Optional.of(e);
            } else {
                log(logger, ctx, "Unrecognized contextObject passed to errorCollector:handleError");
            }
        }

        var error = errorBuilder.build();
        errors.add(error);

        if (exception.isPresent()) {
            log(logger, ctx, "Error added to ErrorCollector" + error.toString(), exception.get());
        } else {
            log(logger, ctx, "Error added to ErrorCollector" + error);
        }
    }

    public void addError(String description, int sequenceNumber) {
        var error = ErrorDTO.builder()
                .timestamp(LocalDateTime.now())
                .description(description)
                .sequenceNumber(sequenceNumber)
                .build();
        errors.add(error);
    }

}
