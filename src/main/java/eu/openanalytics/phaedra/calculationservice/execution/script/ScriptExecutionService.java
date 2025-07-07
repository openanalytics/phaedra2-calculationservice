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
package eu.openanalytics.phaedra.calculationservice.execution.script;

import eu.openanalytics.phaedra.calculationservice.dto.ScriptExecutionInputDTO;
import eu.openanalytics.phaedra.calculationservice.dto.ScriptExecutionOutputDTO;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import eu.openanalytics.phaedra.calculationservice.enumeration.ScriptLanguage;
import eu.openanalytics.phaedra.calculationservice.exception.CalculationException;
import eu.openanalytics.phaedra.calculationservice.service.KafkaProducerService;

@Service
public class ScriptExecutionService {

	private static final int DEFAULT_RETRIES = 3;

	@Autowired
	private KafkaProducerService kafkaProducer;

	@Autowired
	private ObjectMapper objectMapper;

	private ConcurrentHashMap<String, ScriptExecutionRequest> trackedExecutions = new ConcurrentHashMap<>();

	public ScriptExecutionRequest submit(ScriptLanguage lang, String script, String category, Object inputData) {
    	String inputDocument = null;
    	try {
    		inputDocument = objectMapper.writeValueAsString(inputData);
        } catch (JsonProcessingException e) {
        	throw new CalculationException("Failed to serialize input data", e);
        }

    	ScriptExecutionInputDTO input = ScriptExecutionInputDTO.builder()
    			.language(lang.name())
    			.script(script)
					.category(category)
    			.input(inputDocument)
    			.build();

    	ScriptExecutionRequest request = ScriptExecutionRequest.builder()
    			.input(input)
    			.maxRetryCount(DEFAULT_RETRIES)
    			.build();

    	return submit(request);
	}

	public ScriptExecutionRequest submit(ScriptExecutionRequest request) {
		if (request.getId() == null) {
			request.setId(UUID.randomUUID().toString());
			request.getInput().setId(request.getId());
			trackedExecutions.put(request.getId(), request);
		}
		request.setCurrentTry(request.getCurrentTry() + 1);
		kafkaProducer.sendScriptExecutionRequest(request.getInput());
		return request;
	}

	public void handleScriptExecutionUpdate(ScriptExecutionOutputDTO output) {
		ScriptExecutionRequest request = trackedExecutions.get(output.getInputId());
		if (request == null) return;

		if (output.getStatusCode().canBeRetried() && request.getCurrentTry() <= request.getMaxRetryCount()) {
			// Failure but a retry can be attempted
			submit(request);
		} else {
			// Success or non-retryable failure
			request.signalOutputAvailable(output);
			trackedExecutions.remove(request.getId());
		}
	}

}
