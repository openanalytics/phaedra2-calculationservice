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

import static eu.openanalytics.phaedra.calculationservice.util.LoggerHelper.log;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import eu.openanalytics.phaedra.calculationservice.exception.CalculationException;
import eu.openanalytics.phaedra.calculationservice.execution.CalculationContext;
import eu.openanalytics.phaedra.calculationservice.execution.input.CalculationInputHelper;
import eu.openanalytics.phaedra.calculationservice.execution.progress.CalculationStage;
import eu.openanalytics.phaedra.calculationservice.execution.progress.CalculationStateEventCode;
import eu.openanalytics.phaedra.calculationservice.execution.script.ScriptExecutionRequest;
import eu.openanalytics.phaedra.calculationservice.execution.script.ScriptExecutionService;
import eu.openanalytics.phaedra.calculationservice.model.Formula;
import eu.openanalytics.phaedra.calculationservice.model.ModelMapper;
import eu.openanalytics.phaedra.calculationservice.service.KafkaProducerService;
import eu.openanalytics.phaedra.plateservice.dto.WellDTO;
import eu.openanalytics.phaedra.protocolservice.dto.FeatureDTO;
import eu.openanalytics.phaedra.protocolservice.dto.FeatureStatDTO;
import eu.openanalytics.phaedra.resultdataservice.dto.ResultFeatureStatDTO;
import eu.openanalytics.phaedra.scriptengine.dto.ResponseStatusCode;
import eu.openanalytics.phaedra.scriptengine.dto.ScriptExecutionOutputDTO;

/**
 * Feature Stats are sets of statistical values about a feature.
 *
 * Typically this includes min/mean/median/max values for the whole plate,
 * but also stats for each welltype present in the plate.
 *
 * Feature Stats can be calculated as soon as the Feature itself has been calculated.
 * 
 * TODO: wellType is still named "welltype" in protocol-service and resultdata-service. Fix naming across all services
 */
@Service
public class FeatureStatExecutorService {

	private final ScriptExecutionService scriptExecutionService;
    private final KafkaProducerService kafkaProducerService;

    private final ObjectMapper objectMapper;
    private final ModelMapper modelMapper;

    private final Logger logger = LoggerFactory.getLogger(getClass());

    public FeatureStatExecutorService(ObjectMapper objectMapper, ModelMapper modelMapper, KafkaProducerService kafkaProducerService, ScriptExecutionService scriptExecutionService) {
        this.objectMapper = objectMapper;
        this.modelMapper = modelMapper;
        this.kafkaProducerService = kafkaProducerService;
        this.scriptExecutionService = scriptExecutionService;
    }

    public void executeFeatureStats(CalculationContext ctx, FeatureDTO feature) {
        List<FeatureStatDTO> statsToCalculate = ctx.getProtocolData().featureStats.get(feature.getId());
        
        if (statsToCalculate == null || statsToCalculate.isEmpty()) {
        	ctx.getStateTracker().skipStage(feature.getId(), CalculationStage.FeatureStatistics);
            return;
        }
        
        log(logger, ctx, "Calculating %d featureStats for feature %d", statsToCalculate.size(), feature.getId());
        ctx.getStateTracker().startStage(feature.getId(), CalculationStage.FeatureStatistics, statsToCalculate.size());

        // Submit all stat calculation requests
        for (FeatureStatDTO fs: statsToCalculate) {
        	Formula formula = ctx.getProtocolData().formulas.get(fs.getFormulaId());
        	if (formula == null) {
        		ctx.getStateTracker().failStage(feature.getId(), CalculationStage.FeatureStatistics,
        				String.format("Invalid formula ID for stat '%s': %d", fs.getName(), fs.getFormulaId()), feature);
        		return;
        	}

        	Map<String, Object> inputData = collectStatInputData(ctx, feature, fs);
        	ScriptExecutionRequest request = scriptExecutionService.submit(formula.getLanguage(), formula.getFormula(), formula.getCategory().name(), inputData);
        	ctx.getStateTracker().trackScriptExecution(feature.getId(), CalculationStage.FeatureStatistics, fs.getId(), request);
        }

        ctx.getStateTracker().addEventListener(CalculationStage.FeatureStatistics, CalculationStateEventCode.ScriptOutputAvailable, feature.getId(), requests -> {
        	// Accumulate all stats to save.
        	List<ResultFeatureStatDTO> results = new ArrayList<ResultFeatureStatDTO>();
        	requests.entrySet().forEach(req -> {
        		FeatureStatDTO fs = findStat(ctx, feature.getId(), Long.valueOf(req.getKey()));
        		try {
        			results.addAll(parseResults(ctx, feature, fs, req.getValue().getOutput()));
    			} catch (Exception e) {
    				ctx.getStateTracker().failStage(feature.getId(), CalculationStage.FeatureFormula,
    	    				String.format("Feature statistic '%s' failed: %s", fs.getName(), e.getMessage()), feature, e);
    			}
        	});

        	// Reset the stage with size == total nr of stats to save (which is greater than statsToCalculate.size(), e.g. stats for multiple welltypes).
        	ctx.getStateTracker().startStage(feature.getId(), CalculationStage.FeatureStatistics, results.size());
        	results.stream().forEach(res -> kafkaProducerService.sendResultFeatureStats(res.withResultSetId(ctx.getResultSetId())));
        });

        ctx.getStateTracker().addEventListener(CalculationStage.FeatureStatistics, CalculationStateEventCode.Error, feature.getId(), requests -> {
        	requests.entrySet().stream().filter(req -> req.getValue().getOutput().getStatusCode() != ResponseStatusCode.SUCCESS).forEach(req -> {
        		FeatureStatDTO fs = findStat(ctx, feature.getId(), Long.valueOf(req.getKey()));
        		ScriptExecutionOutputDTO output = req.getValue().getOutput();
        		ctx.getErrorCollector().addError(String.format("Feature statistic '%s' failed: %s", fs.getName(), output.getStatusMessage()), output, feature);
        	});
        });
    }

    private FeatureStatDTO findStat(CalculationContext ctx, long featureId, long statId) {
    	var featureStats = ctx.getProtocolData().featureStats.get(featureId);
    	if (featureStats == null) return null;
    	return featureStats.stream().filter(stat -> stat.getId().equals(statId)).findAny().orElse(null);
    }

    private Map<String, Object> collectStatInputData(CalculationContext ctx, FeatureDTO feature, FeatureStatDTO featureStat) {
    	Map<String, Object> input = new HashMap<String, Object>();
    	CalculationInputHelper.addWellInfo(input, ctx, ctx.getWells());
    	// Note: assuming here that the feature values are already sorted by well nr
        input.put("featureValues", ctx.getFeatureResults().get(feature.getId()).getValues());
        input.put("isPlateStat", featureStat.getPlateStat());
        input.put("isWellTypeStat", featureStat.getWelltypeStat());
        return input;
    }

	private List<ResultFeatureStatDTO> parseResults(CalculationContext ctx, FeatureDTO feature, FeatureStatDTO featureStat, ScriptExecutionOutputDTO output) {
		if (output.getStatusCode() != ResponseStatusCode.SUCCESS) throw new CalculationException("Script execution error: %s", output.getStatusMessage());
		
    	List<ResultFeatureStatDTO> results = new ArrayList<>();

    	float plateValue = Float.NaN;
    	Map<String, Float> wellTypeValues = new HashMap<>();
    	
		try {
			// Output string contains a nested 'output' key, e.g.  output = "{\"output\":{\"plateValue\":384,\"wellTypeValues\":{\"HC\":32,\"LC\":32,\"SAMPLE\":320}}}\n"
			Map<?,?> outputMap = objectMapper.readValue(output.getOutput(), Map.class);
			outputMap = (Map<?,?>) outputMap.get("output");
			
			if (outputMap.get("plateValue") instanceof Number) plateValue = ((Number) outputMap.get("plateValue")).floatValue();
			if (outputMap.get("wellTypeValues") instanceof Map) {
				Map<?,?> wellTypeValuesMap = (Map<?,?>) outputMap.get("wellTypeValues");
				wellTypeValuesMap.entrySet().stream().filter(e -> e.getValue() instanceof Number).forEach(e -> wellTypeValues.put((String) e.getKey(), ((Number) e.getValue()).floatValue()));
			}
		} catch (JsonProcessingException e) {
			throw new CalculationException("Invalid response JSON", e);
		}

		if (featureStat.getPlateStat()) {
			results.add(createResultStatDTO(feature, featureStat, output, plateValue, null));
		}
		
		if (featureStat.getWelltypeStat()) {
			List<String> wellTypes = ctx.getWells().stream().map(WellDTO::getWellType).distinct().toList();
            for (String wellType : wellTypes) {
            	Float numValue = Optional.ofNullable(wellTypeValues.get(wellType)).orElse(Float.NaN);
                results.add(createResultStatDTO(feature, featureStat, output, numValue, wellType));
            }
		}

    	return results;
    }

    private ResultFeatureStatDTO createResultStatDTO(FeatureDTO feature, FeatureStatDTO featureStat, ScriptExecutionOutputDTO output, Float value, String wellType) {
    	return ResultFeatureStatDTO.builder()
	        .featureId(feature.getId())
	        .featureStatId(featureStat.getId())
	        .value(value)
	        .statisticName(featureStat.getName())
	        .welltype(wellType)
	        .statusCode(modelMapper.map(output.getStatusCode()))
	        .statusMessage(output.getStatusMessage())
	        .build();
    }
}
