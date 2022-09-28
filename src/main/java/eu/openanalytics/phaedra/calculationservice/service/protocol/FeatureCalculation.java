/**
 * Phaedra II
 *
 * Copyright (C) 2016-2022 Open Analytics
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
package eu.openanalytics.phaedra.calculationservice.service.protocol;

import eu.openanalytics.phaedra.calculationservice.model.Feature;
import eu.openanalytics.phaedra.scriptengine.client.model.ScriptExecution;
import eu.openanalytics.phaedra.scriptengine.dto.ScriptExecutionOutputDTO;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class FeatureCalculation {

    private final Future<Optional<ScriptExecution>> scriptExecutionFuture;
    private final Feature feature;

    private Optional<ScriptExecution> scriptExecution = Optional.empty();
    private Optional<ScriptExecutionOutputDTO> output = Optional.empty();

    public FeatureCalculation(Feature feature, Future<Optional<ScriptExecution>> scriptExecutionFuture) {
        Objects.requireNonNull(feature, "Feature cannot be null");
        Objects.requireNonNull(scriptExecutionFuture, "scriptExecutionFuture cannot be null");
        this.feature = feature;
        this.scriptExecutionFuture = scriptExecutionFuture;
    }

    public void waitForExecution() throws ExecutionException, InterruptedException {
        scriptExecution = scriptExecutionFuture.get();
    }

    public void waitForOutput() throws ExecutionException, InterruptedException {
        if (scriptExecution.isPresent()) {
            output = Optional.of(scriptExecution.get().getOutput().get());
        }
    }

    public Feature getFeature() {
        return feature;
    }

    public Optional<ScriptExecutionOutputDTO> getOutput() {
        return output;
    }
}
