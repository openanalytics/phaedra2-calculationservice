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
package eu.openanalytics.phaedra.calculationservice.service.protocol;

import static eu.openanalytics.phaedra.calculationservice.util.LoggerHelper.log;

import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import eu.openanalytics.phaedra.calculationservice.model.CalculationContext;
import eu.openanalytics.phaedra.calculationservice.service.KafkaProducerService;
import eu.openanalytics.phaedra.plateservice.client.PlateServiceClient;
import eu.openanalytics.phaedra.plateservice.client.exception.PlateUnresolvableException;
import eu.openanalytics.phaedra.plateservice.dto.PlateCalculationStatusDTO;
import eu.openanalytics.phaedra.plateservice.enumartion.CalculationStatus;
import eu.openanalytics.phaedra.protocolservice.client.exception.ProtocolUnresolvableException;
import eu.openanalytics.phaedra.resultdataservice.client.ResultDataServiceClient;
import eu.openanalytics.phaedra.resultdataservice.client.exception.ResultSetUnresolvableException;
import eu.openanalytics.phaedra.resultdataservice.dto.ResultDataDTO;
import eu.openanalytics.phaedra.resultdataservice.dto.ResultFeatureStatDTO;
import eu.openanalytics.phaedra.resultdataservice.dto.ResultSetDTO;
import eu.openanalytics.phaedra.resultdataservice.enumeration.StatusCode;

/**
 * This service is responsible for executing, and tracking the progress of execution for,
 * an entire protocol on a plate. This is also called "plate calculation".
 * 
 * A protocol consists of a list of features, which must be executed in the correct order.
 * Therefore, the features are grouped into "sequences", and each sequence must be completed
 * before the next sequence can start.
 */
@Service
public class ProtocolExecutorService {

	private final FeatureExecutorService featureExecutorService;
	
    private final ResultDataServiceClient resultDataServiceClient;
    private final PlateServiceClient plateServiceClient;

    private final ProtocolDataCollector protocolDataCollector;
    private final KafkaProducerService kafkaProducerService;
    
    private final Map<Long, CalculationContext> activeContexts = new ConcurrentHashMap<>();
    
    private final Logger logger = LoggerFactory.getLogger(getClass());

    public ProtocolExecutorService(
    		FeatureExecutorService featureExecutorService,
    		ResultDataServiceClient resultDataServiceClient,
    		ProtocolDataCollector protocolDataCollector,
    		PlateServiceClient plateServiceClient,
    		KafkaProducerService kafkaProducerService) {
    	
    	this.featureExecutorService= featureExecutorService; 
        this.resultDataServiceClient = resultDataServiceClient;
        this.protocolDataCollector = protocolDataCollector;
        this.plateServiceClient = plateServiceClient;
        this.kafkaProducerService = kafkaProducerService;
    }

    public Future<Long> execute(long protocolId, long plateId, long measId) {
        var resultSetIdFuture = new CompletableFuture<Long>();
        ForkJoinPool.commonPool().submit(() -> {
            try {
                triggerProtocolExecution(resultSetIdFuture, protocolId, plateId, measId);
            } catch (Throwable ex) {
            	logger.error("Unexpected error during protocol calculation", ex);
                ex.printStackTrace();
            }
        });
        return resultSetIdFuture;
    }

    private void triggerProtocolExecution(CompletableFuture<Long> resultSetIdFuture, long protocolId, long plateId, long measId) throws ProtocolUnresolvableException, ResultSetUnresolvableException, PlateUnresolvableException {
    	// Collect all required input data and create a ResultSet instance
        var protocolData = protocolDataCollector.getProtocolData(protocolId);
        var plate = plateServiceClient.getPlate(plateId);
        var wells = plateServiceClient.getWells(plateId);
        var resultSet = resultDataServiceClient.createResultDataSet(protocolId, plateId, measId);
        resultSetIdFuture.complete(resultSet.getId());
        
        CalculationContext ctx = CalculationContext.newInstance(protocolData, plate, wells, resultSet.getId(), measId);
        log(logger, ctx, "Executing protocol %d", protocolId);
        emitPlateCalcStatus(plateId, CalculationStatus.CALCULATION_IN_PROGRESS);
        activeContexts.put(resultSet.getId(), ctx);

        // Start the first sequence
        triggerSequenceExecution(ctx, ctx.getCalculationProgress().getCurrentSequence());
    }
    
    private void triggerSequenceExecution(CalculationContext ctx, Integer sequence) {
    	log(logger, ctx, "Executing sequence %d", sequence);
    	ctx.getProtocolData().protocol.getFeatures().parallelStream()
        		.filter(f -> f.getSequence() == sequence)
        		.map(f -> featureExecutorService.executeFeature(ctx, f, sequence))
        		.filter(r -> r != null)
        		.toList();
    }
    
    public void handleResultSetUpdate(Object resultObject) {
    	Long rsId = null;
    	if (resultObject instanceof ResultSetDTO) {
    		rsId = ((ResultSetDTO) resultObject).getId();
    	} else if (resultObject instanceof ResultDataDTO) {
    		rsId = ((ResultDataDTO) resultObject).getResultSetId();
    	} else if (resultObject instanceof ResultFeatureStatDTO) {
    		rsId = ((ResultFeatureStatDTO) resultObject).getResultSetId();
    	}
    	if (rsId == null) return;
    	
    	CalculationContext ctx = activeContexts.get(rsId);
    	if (ctx == null) return;
    	
    	// Update progress information
    	log(logger, ctx, "Result data received: %d", resultObject);
    	ctx.getCalculationProgress().updateProgress(resultObject);
    	log(logger, ctx, "Calculation progress: %d", ctx.getCalculationProgress().getCompletedFraction());
    	
    	if (ctx.getCalculationProgress().isComplete()) {
    		handleCalculationEnded(ctx);
    	} else if (ctx.getCalculationProgress().isCurrentSequenceComplete()) {
    		if (ctx.getErrorCollector().hasError()) {
    			handleCalculationEnded(ctx);
    		} else {
    			ctx.getCalculationProgress().incrementCurrentSequence();
    			triggerSequenceExecution(ctx, ctx.getCalculationProgress().getCurrentSequence());
    		}
    	}
    }
    
    private ResultSetDTO handleCalculationEnded(CalculationContext ctx) {
    	activeContexts.remove(ctx.getResultSetId());
    	
    	ResultSetDTO rs = null;
        if (ctx.getErrorCollector().hasError()) {
        	logger.warn("Calculation failed with errors:\n" + ctx.getErrorCollector().getErrorDescription());
            emitPlateCalcStatus(ctx.getPlate().getId(), CalculationStatus.CALCULATION_ERROR);
			try {
				rs = resultDataServiceClient.completeResultDataSet(ctx.getResultSetId(), StatusCode.FAILURE, ctx.getErrorCollector().getErrors(), ctx.getErrorCollector().getErrorDescription());
			} catch (ResultSetUnresolvableException e) {
				logger.error("Unexpected error while updating result set", e);
			}
        } else {
        	log(logger, ctx, "Calculation finished: SUCCESS");
        	emitPlateCalcStatus(ctx.getPlate().getId(), CalculationStatus.CALCULATION_OK);
			try {
				rs = resultDataServiceClient.completeResultDataSet(ctx.getResultSetId(), StatusCode.SUCCESS, new ArrayList<>(), "");
			} catch (ResultSetUnresolvableException e) {
				logger.error("Unexpected error while updating result set", e);
			}
        }
        return rs;
    }
    
    private void emitPlateCalcStatus(Long plateId, CalculationStatus calculationStatus) {
        PlateCalculationStatusDTO plateCalcStatus = PlateCalculationStatusDTO.builder().plateId(plateId).calculationStatus(calculationStatus).build();
        kafkaProducerService.sendPlateCalculationStatus(plateCalcStatus);
    }

}
