/**
 * Phaedra II
 *
 * Copyright (C) 2016-2024 Open Analytics
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
package eu.openanalytics.phaedra.calculationservice.execution.script;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ForkJoinPool;
import java.util.function.Consumer;

import eu.openanalytics.phaedra.scriptengine.dto.ScriptExecutionInputDTO;
import eu.openanalytics.phaedra.scriptengine.dto.ScriptExecutionOutputDTO;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class ScriptExecutionRequest {

	private String id;

	private ScriptExecutionInputDTO input;
	private volatile ScriptExecutionOutputDTO output;

	private int currentTry;
	private int maxRetryCount;

	private List<Consumer<ScriptExecutionOutputDTO>> callbacks;

	public synchronized ScriptExecutionRequest addCallback(Consumer<ScriptExecutionOutputDTO> callback) {
		if (callbacks == null) callbacks = new ArrayList<>();
		callbacks.add(callback);
		return this;
	}

	public void signalOutputAvailable(ScriptExecutionOutputDTO output) {
		this.output = output;
        if (callbacks != null) {
        	ForkJoinPool.commonPool().submit(() -> callbacks.forEach(c -> c.accept(output)));
        }
	}
}
