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
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import eu.openanalytics.phaedra.calculationservice.dto.event.CalculationEvent;
import eu.openanalytics.phaedra.calculationservice.execution.CalculationContext;
import eu.openanalytics.phaedra.calculationservice.execution.progress.CalculationStage;
import eu.openanalytics.phaedra.calculationservice.execution.progress.CalculationStateEventCode;
import eu.openanalytics.phaedra.calculationservice.service.KafkaProducerService;
import eu.openanalytics.phaedra.plateservice.client.PlateServiceClient;
import eu.openanalytics.phaedra.plateservice.client.exception.PlateUnresolvableException;
import eu.openanalytics.phaedra.plateservice.dto.PlateCalculationStatusDTO;
import eu.openanalytics.phaedra.plateservice.enumeration.CalculationStatus;
import eu.openanalytics.phaedra.protocolservice.client.exception.ProtocolUnresolvableException;
import eu.openanalytics.phaedra.resultdataservice.client.ResultDataServiceClient;
import eu.openanalytics.phaedra.resultdataservice.client.exception.ResultSetUnresolvableException;
import eu.openanalytics.phaedra.resultdataservice.enumeration.StatusCode;

/**
 * This service is responsible for executing an entire protocol on a plate-measurement.
 * This is also called "plate calculation".
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

    /**
     * Process the update of a ResultSet. Should only be called by Kafka message processing.
     */
    public void handleResultSetUpdate(Long resultSetId, Object payload) {
    	CalculationContext ctx = activeContexts.get(resultSetId);
    	if (ctx == null) return;
    	ctx.getStateTracker().handleResultSetUpdate(payload);
    }

    private void triggerProtocolExecution(CompletableFuture<Long> resultSetIdFuture, long protocolId, long plateId, long measId) throws ProtocolUnresolvableException, ResultSetUnresolvableException, PlateUnresolvableException {
    	// Collect all required input data and create a ResultSet instance
        var protocolData = protocolDataCollector.getProtocolData(protocolId);
        var plate = plateServiceClient.getPlate(plateId);
        var wells = plateServiceClient.getWells(plateId);

        var resultSet = resultDataServiceClient.createResultDataSet(protocolId, plateId, measId);
        resultSetIdFuture.complete(resultSet.getId());

        CalculationContext ctx = CalculationContext.create(protocolData, plate, wells, resultSet.getId(), measId);
        activeContexts.put(resultSet.getId(), ctx);
        emitCalculationEvent(ctx, CalculationStatus.CALCULATION_IN_PROGRESS);

        ctx.getStateTracker().addEventListener(CalculationStage.Sequence, CalculationStateEventCode.Complete, null, req -> triggerNextSequence(ctx));
        ctx.getStateTracker().addEventListener(CalculationStage.Sequence, CalculationStateEventCode.Error, null, req -> handleCalculationEnded(ctx));
        ctx.getStateTracker().addEventListener(CalculationStage.Protocol, CalculationStateEventCode.Complete, null, req -> handleCalculationEnded(ctx));

        // Trigger the first sequence now
        triggerNextSequence(ctx);
    }

    private void triggerNextSequence(CalculationContext ctx) {
    	ctx.getStateTracker().incrementCurrentSequence();
    	ctx.getProtocolData().protocol.getFeatures().parallelStream()
    		.filter(f -> f.getSequence() == ctx.getStateTracker().getCurrentSequence())
    		.forEach(f -> featureExecutorService.executeFeature(ctx, f));
    }

    private void handleCalculationEnded(CalculationContext ctx) {
    	activeContexts.remove(ctx.getResultSetId());
    	try {
	        if (ctx.getErrorCollector().hasError()) {
	        	logger.warn("Calculation failed with errors:\n" + ctx.getErrorCollector().getErrorDescription());
	        	emitCalculationEvent(ctx, CalculationStatus.CALCULATION_ERROR);
				resultDataServiceClient.completeResultDataSet(ctx.getResultSetId(), StatusCode.FAILURE, ctx.getErrorCollector().getErrors(), ctx.getErrorCollector().getErrorDescription());
	        } else {
	        	log(logger, ctx, "Calculation finished: SUCCESS");
	        	emitCalculationEvent(ctx, CalculationStatus.CALCULATION_OK);
				resultDataServiceClient.completeResultDataSet(ctx.getResultSetId(), StatusCode.SUCCESS, new ArrayList<>(), "");
	        }
    	} catch (ResultSetUnresolvableException e) {
    		logger.error("Unexpected error while updating result set", e);
    	}
    }

    private void emitCalculationEvent(CalculationContext ctx, CalculationStatus calculationStatus) {
    	CalculationEvent event = CalculationEvent.builder()
    			.plateId(ctx.getPlate().getId())
    			.measurementId(ctx.getMeasId())
    			.protocolId(ctx.getProtocolData().protocol.getId())
    			.calculationStatus(calculationStatus)
    			.build();
    	kafkaProducerService.notifyCalculationEvent(event);

    	//TODO: deprecate, PlateService should update its own state by consuming CalculationEvents
    	PlateCalculationStatusDTO plateCalcStatus = PlateCalculationStatusDTO.builder()
    			.plateId(ctx.getPlate().getId())
    			.calculationStatus(calculationStatus).build();
    	kafkaProducerService.sendPlateCalculationStatus(plateCalcStatus);
    }
}
