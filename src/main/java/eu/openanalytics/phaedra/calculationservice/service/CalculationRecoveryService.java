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
package eu.openanalytics.phaedra.calculationservice.service;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.concurrent.ExecutionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import eu.openanalytics.phaedra.calculationservice.service.protocol.ProtocolExecutorService;
import eu.openanalytics.phaedra.resultdataservice.client.ResultDataServiceClient;
import eu.openanalytics.phaedra.resultdataservice.client.exception.ResultSetUnresolvableException;
import eu.openanalytics.phaedra.resultdataservice.dto.ErrorDTO;
import eu.openanalytics.phaedra.resultdataservice.enumeration.StatusCode;

@Service
public class CalculationRecoveryService {

    private final ResultDataServiceClient resultDataServiceClient;
    private final ProtocolExecutorService protocolExecutorService;

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private static final Duration MAX_RECOVER_TIME = Duration.ofHours(2);

    public CalculationRecoveryService(ResultDataServiceClient resultDataServiceClient, ProtocolExecutorService protocolExecutorService) {
        this.resultDataServiceClient = resultDataServiceClient;
        this.protocolExecutorService = protocolExecutorService;
    }

    //TODO: Enable this ASAP
//    @EventListener(ApplicationReadyEvent.class)
    public void recoverCalculations() throws ResultSetUnresolvableException, ExecutionException, InterruptedException {
        var startTime = LocalDateTime.now().minus(MAX_RECOVER_TIME);
        var resultSets = resultDataServiceClient
                .getResultSet(StatusCode.SCHEDULED)
                .stream().filter(it -> it.getExecutionStartTimeStamp().isAfter(startTime))
                .toList();

        logger.info("Found {} calculation to retry", resultSets.size());

        for (var resultSet : resultSets) {
            var r = protocolExecutorService.execute(resultSet.getProtocolId(), resultSet.getPlateId(), resultSet.getMeasId());
            var newResultSetId = r.resultSetId().get();
            var error = ErrorDTO.builder()
                    .timestamp(LocalDateTime.now())
                    .description("Calculation re-scheduled because CalculationService was restarted")
                    .newResultSetId(newResultSetId)
                    .build();
            resultDataServiceClient.completeResultDataSet(resultSet.getId(), StatusCode.FAILURE,
                    List.of(error), error.toString());
        }

    }
}
