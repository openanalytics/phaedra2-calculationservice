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
package eu.openanalytics.phaedra.calculationservice.service.protocol;

import java.util.Set;
import java.util.stream.Collectors;

import org.springframework.stereotype.Service;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.openanalytics.phaedra.calculationservice.enumeration.CalculationScope;
import eu.openanalytics.phaedra.calculationservice.exception.CalculationException;
import eu.openanalytics.phaedra.calculationservice.execution.CalculationContext;
import eu.openanalytics.phaedra.calculationservice.execution.input.DefaultInputGroupingStrategy;
import eu.openanalytics.phaedra.calculationservice.execution.input.InputGroup;
import eu.openanalytics.phaedra.calculationservice.execution.input.InputGroupingStrategy;
import eu.openanalytics.phaedra.calculationservice.execution.progress.CalculationStage;
import eu.openanalytics.phaedra.calculationservice.execution.progress.CalculationStateEventCode;
import eu.openanalytics.phaedra.calculationservice.execution.script.ScriptExecutionRequest;
import eu.openanalytics.phaedra.calculationservice.execution.script.ScriptExecutionService;
import eu.openanalytics.phaedra.calculationservice.model.Formula;
import eu.openanalytics.phaedra.calculationservice.model.ModelMapper;
import eu.openanalytics.phaedra.calculationservice.service.KafkaProducerService;
import eu.openanalytics.phaedra.measurementservice.client.MeasurementServiceClient;
import eu.openanalytics.phaedra.protocolservice.dto.FeatureDTO;
import eu.openanalytics.phaedra.resultdataservice.client.ResultDataServiceClient;
import eu.openanalytics.phaedra.resultdataservice.dto.ResultDataDTO;
import eu.openanalytics.phaedra.scriptengine.dto.ResponseStatusCode;

/**
 * Feature execution is a part of the protocol execution procedure.
 * 
 * One feature is calculated by evaluating one formula. This formula
 * may reference data from other features, or from the plate's "raw"
 * measurement columns.
 * 
 * Formula evaluation is offloaded to an appropriate ScriptEngine that
 * supports the language used by the formula.
 */
@Service
public class FeatureExecutorService {

    private final FeatureStatExecutorService featureStatExecutorService;
    private final CurveFittingExecutorService curveFitExecutorService;
    private final ScriptExecutionService scriptExecutionService;
    private final KafkaProducerService kafkaProducerService;
    private final InputGroupingStrategy inputGroupingStrategy;
    
    public FeatureExecutorService(
    		MeasurementServiceClient measurementServiceClient, 
    		ResultDataServiceClient resultDataServiceClient,
    		FeatureStatExecutorService featureStatExecutorService,
    		CurveFittingExecutorService curveFitExecutorService,
    		ScriptExecutionService scriptExecutionService,
    		KafkaProducerService kafkaProducerService,
    		ModelMapper modelMapper,
    		ObjectMapper objectMapper) {
    	
        this.featureStatExecutorService = featureStatExecutorService;
        this.curveFitExecutorService = curveFitExecutorService;
        this.scriptExecutionService = scriptExecutionService;
        this.kafkaProducerService = kafkaProducerService;
        this.inputGroupingStrategy = new DefaultInputGroupingStrategy(measurementServiceClient, resultDataServiceClient, modelMapper, objectMapper);
    }

    /**
     * Calculate the value for a feature.
     * This operation does not block, and will return as soon as the request is launched.
     */
    public void executeFeature(CalculationContext ctx, FeatureDTO feature) {
    	
    	// Retrieve and validate the formula
    	Formula formula = ctx.getProtocolData().formulas.get(feature.getFormulaId());
    	if (formula.getScope() != CalculationScope.WELL) {
    		ctx.getErrorCollector().addError("Invalid formula scope for feature calculation", feature, formula);
    		return;
    	}
    	
    	try {
    		// Gather input, and split in groups as needed
    		Set<InputGroup> groups = inputGroupingStrategy.createGroups(ctx, feature);
    		ctx.getStateTracker().startStage(feature.getId(), CalculationStage.FeatureFormula, groups.size());
    		
    		// Submit a script execution for each group
    		for (InputGroup group: groups) {
    			ScriptExecutionRequest request = scriptExecutionService.submit(formula.getLanguage(), formula.getFormula(), group.getInputVariables());
        		ctx.getStateTracker().trackScriptExecution(feature.getId(), CalculationStage.FeatureFormula, request, null);    			
    		}
    	} catch (CalculationException e) {
    		// Expected errors have already been added to the ErrorCollector at this point.
    		return;
    	} catch (Exception e) {
    		// Unexpected errors are registered here.
    		ctx.getErrorCollector().addError(String.format("Failed to schedule feature calculation: %s", e.getMessage()), feature, formula, e);
        	return;
    	}
    	
    	ctx.getStateTracker().addEventListener(CalculationStage.FeatureFormula, CalculationStateEventCode.ScriptOutputAvailable, feature.getId(), requests -> {
        	ResultDataDTO resultData = inputGroupingStrategy.mergeOutput(ctx, feature, requests.stream().map(req -> req.getOutput()).collect(Collectors.toSet()));
    		kafkaProducerService.sendResultData(resultData);
    		ctx.getFeatureResults().put(feature.getId(), resultData);
    	});
    	
    	ctx.getStateTracker().addEventListener(CalculationStage.FeatureFormula, CalculationStateEventCode.Complete, feature.getId(), requests -> {
    		featureStatExecutorService.executeFeatureStats(ctx, feature);
    		curveFitExecutorService.execute(ctx, feature);
    	});
    	
    	ctx.getStateTracker().addEventListener(CalculationStage.FeatureFormula, CalculationStateEventCode.Error, feature.getId(), requests -> {
    		requests.stream().map(req -> req.getOutput())
    			.filter(o -> o.getStatusCode() != ResponseStatusCode.SUCCESS)
    			.forEach(o -> ctx.getErrorCollector().addError(String.format("Script execution failed with status %s", o.getStatusCode()), o, feature, formula));
    	});
    }
}

