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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import eu.openanalytics.phaedra.calculationservice.model.CalculationContext;
import eu.openanalytics.phaedra.calculationservice.service.KafkaProducerService;
import eu.openanalytics.phaedra.calculationservice.util.ErrorCollector;
import eu.openanalytics.phaedra.plateservice.client.PlateServiceClient;
import eu.openanalytics.phaedra.plateservice.client.exception.PlateUnresolvableException;
import eu.openanalytics.phaedra.plateservice.dto.PlateCalculationStatusDTO;
import eu.openanalytics.phaedra.plateservice.enumartion.CalculationStatus;
import eu.openanalytics.phaedra.protocolservice.client.exception.ProtocolUnresolvableException;
import eu.openanalytics.phaedra.resultdataservice.client.ResultDataServiceClient;
import eu.openanalytics.phaedra.resultdataservice.client.exception.ResultSetUnresolvableException;
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

    private final SequenceExecutorService sequenceExecutorService;
    private final ProtocolDataCollector protocolDataCollector;
    
    private final ResultDataServiceClient resultDataServiceClient;
    private final PlateServiceClient plateServiceClient;

    private final ExecutorService executorService;
    private final KafkaProducerService kafkaProducerService;
    private final Logger logger = LoggerFactory.getLogger(getClass());

    public ProtocolExecutorService(ResultDataServiceClient resultDataServiceClient, SequenceExecutorService sequenceExecutorService, ProtocolDataCollector protocolDataCollector, PlateServiceClient plateServiceClient, KafkaProducerService kafkaProducerService) {
        this.resultDataServiceClient = resultDataServiceClient;
        this.sequenceExecutorService = sequenceExecutorService;
        this.protocolDataCollector = protocolDataCollector;
        this.plateServiceClient = plateServiceClient;
        this.kafkaProducerService = kafkaProducerService;

        executorService = Executors.newCachedThreadPool();
    }

    public record ProtocolExecution(CompletableFuture<Long> resultSetId, Future<ResultSetDTO> resultSet) {};

    public ProtocolExecution execute(long protocolId, long plateId, long measId) {
        var resultSetIdFuture = new CompletableFuture<Long>();
        return new ProtocolExecution(resultSetIdFuture, executorService.submit(() -> {
            try {
                return executeProtocol(resultSetIdFuture, protocolId, plateId, measId);
            } catch (Throwable ex) {
                // print the stack strace. Since the future may never be awaited, we may not see the error otherwise
                ex.printStackTrace();
                throw ex;
            }
        }));
    }

    public ResultSetDTO executeProtocol(CompletableFuture<Long> resultSetIdFuture, long protocolId, long plateId, long measId) throws ProtocolUnresolvableException, ResultSetUnresolvableException, PlateUnresolvableException {
    	
        var protocolData = protocolDataCollector.getProtocolData(protocolId);
        var plate = plateServiceClient.getPlate(plateId);
        var wells = plateServiceClient.getWells(plateId);
        
        var resultSet = resultDataServiceClient.createResultDataSet(protocolId, plateId, measId);
        resultSetIdFuture.complete(resultSet.getId());
        
        // Create a CalculationContext holding all the entities related to the calculation
        var ctx = CalculationContext.newInstance(protocolData, plate, wells, resultSet.getId(), measId);
        log(logger, ctx,  "Starting calculation");

        emitPlateCalcStatus(plateId, CalculationStatus.CALCULATION_IN_PROGRESS);

        // Execute every sequence
        for (Integer seq: protocolData.sequences.keySet().stream().sorted().toList()) {
            boolean success = sequenceExecutorService.executeSequence(ctx, executorService, seq);
            if (!success) break;
        }

        log(logger, ctx, "Waiting for FeatureStat calculations");
        for (var featureStat : ctx.getComputedStatsForFeature().entrySet()) {
            try {
                featureStat.getValue().get();
            } catch (InterruptedException e) {
                // ideally these exceptions should be caught and handled in the FeatureStatExecutor service, however
                // we still need to catch them here, because of the API design of Future.
                ctx.getErrorCollector().addError("executing protocol => waiting for FeatureStat calculations to complete => interrupted", e, featureStat.getKey());
            } catch (ExecutionException e) {
                ctx.getErrorCollector().addError("executing protocol => waiting for FeatureStat calculations to complete => exception during execution", e.getCause(), featureStat.getKey());
            } catch (Throwable e) {
                ctx.getErrorCollector().addError("executing protocol => waiting for FeatureStat calculations to complete => exception during execution", e, featureStat.getKey());
            }
        }

        if (ctx.getErrorCollector().hasError()) {
            return saveError(resultSet, ctx, ctx.getErrorCollector());
        }

        return saveSuccess(resultSet, ctx);
    }
    
    private ResultSetDTO saveSuccess(ResultSetDTO resultSet, CalculationContext calculationContext) throws ResultSetUnresolvableException, PlateUnresolvableException {
        log(logger, calculationContext, "Calculation finished: SUCCESS");
        ResultSetDTO resultSetDTO = resultDataServiceClient.completeResultDataSet(resultSet.getId(), StatusCode.SUCCESS, new ArrayList<>(), "");
        emitPlateCalcStatus(resultSet.getPlateId(), CalculationStatus.CALCULATION_OK);
        return resultSetDTO;
    }

    private ResultSetDTO saveError(ResultSetDTO resultSet, CalculationContext calculationContext, ErrorCollector errorCollector) throws ResultSetUnresolvableException, PlateUnresolvableException {
        logger.warn("Protocol failed with errorDescription\n" + errorCollector.getErrorDescription());
        ResultSetDTO resultSetDTO = resultDataServiceClient.completeResultDataSet(resultSet.getId(), StatusCode.FAILURE, errorCollector.getErrors(), errorCollector.getErrorDescription());
        emitPlateCalcStatus(resultSet.getPlateId(), CalculationStatus.CALCULATION_ERROR);
        return resultSetDTO;
    }

    // TODO: Secure kafka producer
    private void emitPlateCalcStatus(Long plateId, CalculationStatus calculationStatus) {
        PlateCalculationStatusDTO plateCalcStatus = PlateCalculationStatusDTO.builder().plateId(plateId).calculationStatus(calculationStatus).build();
        kafkaProducerService.sendPlateCalculationStatus(plateCalcStatus);
    }

}
