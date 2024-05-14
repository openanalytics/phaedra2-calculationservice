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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import eu.openanalytics.phaedra.calculationservice.exception.CalculationException;
import eu.openanalytics.phaedra.calculationservice.execution.CalculationContext;
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
        log(logger, ctx, "Calculating %d featureStats for feature %d", statsToCalculate.size(), feature.getId());
        ctx.getStateTracker().startStage(feature.getId(), CalculationStage.FeatureStatistics, statsToCalculate.size());
        
        // Submit all stat calculation requests
        for (FeatureStatDTO fs: statsToCalculate) {
        	Formula formula = ctx.getProtocolData().formulas.get(fs.getFormulaId());
        	Map<String, Object> inputData = collectStatInputData(ctx, feature, fs);

        	ScriptExecutionRequest request = scriptExecutionService.submit(formula.getLanguage(), formula.getFormula(), inputData);
        	ctx.getStateTracker().trackScriptExecution(feature.getId(), CalculationStage.FeatureStatistics, request, Collections.singletonMap("stat", fs));
        }
        
        ctx.getStateTracker().addEventListener(CalculationStage.FeatureStatistics, CalculationStateEventCode.ScriptOutputAvailable, feature.getId(), requests -> {
        	for (ScriptExecutionRequest request: requests) {
    			try {
    				FeatureStatDTO fs = (FeatureStatDTO) ctx.getStateTracker().getRequestContext(request).get("stat");
    				List<ResultFeatureStatDTO> results = parseResults(ctx, feature, fs, request.getOutput());
    				kafkaProducerService.sendResultFeatureStats(ctx.getResultSetId(), results);
    			} catch (CalculationException e) {
    				ctx.getErrorCollector().addError(String.format("Feature statistic calculation failed: %s", e.getMessage()), e, feature);
    			}
    		}
        });
        
        ctx.getStateTracker().addEventListener(CalculationStage.FeatureStatistics, CalculationStateEventCode.Error, feature.getId(), requests -> {
        	requests.stream().map(req -> req.getOutput())
	    		.filter(o -> o.getStatusCode() != ResponseStatusCode.SUCCESS)
	    		.forEach(o -> ctx.getErrorCollector().addError(String.format("Feature statistic calculation failed with status %s", o.getStatusCode()), o, feature));
        });
    }

    private Map<String, Object> collectStatInputData(CalculationContext ctx, FeatureDTO feature, FeatureStatDTO featureStat) {
    	Map<String, Object> input = new HashMap<String, Object>();
        input.put("lowWelltype", ctx.getProtocolData().protocol.getLowWelltype());
        input.put("highWelltype", ctx.getProtocolData().protocol.getHighWelltype());
        input.put("welltypes", ctx.getWells().stream().map(WellDTO::getWellType).toList());
        input.put("featureValues", ctx.getFeatureResults().get(feature.getId()).getValues());
        input.put("isPlateStat", featureStat.getPlateStat());
        input.put("isWelltypeStat", featureStat.getWelltypeStat());
        return input;
    }
    
    private List<ResultFeatureStatDTO> parseResults(CalculationContext ctx, FeatureDTO feature, FeatureStatDTO featureStat, ScriptExecutionOutputDTO output) {
    	List<ResultFeatureStatDTO> results = new ArrayList<>();

    	float plateValue = Float.NaN;
    	Map<String, Float> wellTypeValues = null;
		try {
			OutputWrapper outputWrapper = objectMapper.readValue(output.getOutput(), OutputWrapper.class);
			plateValue = outputWrapper.getPlateValue().orElse(Float.NaN);
			wellTypeValues = outputWrapper.getWelltypeOutputs();
		} catch (JsonProcessingException e) {
			throw new CalculationException("Invalid response JSON", e);
		}

		if (featureStat.getPlateStat()) {
			results.add(createResultStatDTO(feature, featureStat, output, plateValue, null));
		} else if (featureStat.getWelltypeStat()) {
			List<String> wellTypes = ctx.getWells().stream().map(WellDTO::getWellType).distinct().toList();
            for (String wellType : wellTypes) {
            	Float numValue = Optional.ofNullable(wellTypeValues.get(wellType)).orElse(Float.NaN);
                results.add(createResultStatDTO(feature, featureStat, output, numValue, wellType));
            }
		} else {
			throw new CalculationException(String.format("Invalid feature stat: %s", featureStat.getName()), feature);
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
	        .exitCode(output.getExitCode())
	        .build();
    }

    private static class OutputWrapper {
    	
        private final Float plateValue;
        private final Map<String, Float> welltypeValues;

        @JsonCreator
        private OutputWrapper(
                @JsonProperty(value = "plateValue", required = true) Float plateValue,
                @JsonProperty(value = "welltypeValues", required = true) Map<String, Float> welltypeValues) {
            this.plateValue = plateValue;
            this.welltypeValues = welltypeValues;
        }

        public Optional<Float> getPlateValue() {
            return Optional.ofNullable(plateValue);
        }


        public Map<String, Float> getWelltypeOutputs() {
            return welltypeValues;
        }

    }

}
