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
package eu.openanalytics.phaedra.calculationservice;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.matches;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.LocalDateTime;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import eu.openanalytics.phaedra.calculationservice.service.CalculationRecoveryService;
import eu.openanalytics.phaedra.calculationservice.service.TokenService;
import eu.openanalytics.phaedra.calculationservice.service.protocol.ProtocolExecutorService;
import eu.openanalytics.phaedra.resultdataservice.client.ResultDataServiceClient;
import eu.openanalytics.phaedra.resultdataservice.dto.ErrorDTO;
import eu.openanalytics.phaedra.resultdataservice.dto.ResultSetDTO;
import eu.openanalytics.phaedra.resultdataservice.enumeration.StatusCode;

@Disabled
public class CalculationRecoveryTest {

    static TokenService tokenService;


    @BeforeAll
    static void init() {
        tokenService = new TokenService();
    }

    @Test
    public void simpleTest() throws Exception {
        var resultDataServiceClient = mock(ResultDataServiceClient.class);
        var protocolExecutorService = mock(ProtocolExecutorService.class);
        var calculationRecoveryService = new CalculationRecoveryService(resultDataServiceClient, protocolExecutorService, tokenService);

        when(resultDataServiceClient.getResultSet(StatusCode.SCHEDULED)).thenReturn(List.of(
                // this ResultSet is too old and should not be retried
                ResultSetDTO.builder()
                        .id(42L)
                        .executionStartTimeStamp(LocalDateTime.now().minusHours(2))
                        .outcome(StatusCode.SCHEDULED)
                        .protocolId(215L)
                        .plateId(15L)
                        .measId(1361L)
                        .build(),
                ResultSetDTO.builder()
                        .id(43L)
                        .executionStartTimeStamp(LocalDateTime.now().minusMinutes(80))
                        .outcome(StatusCode.SCHEDULED)
                        .protocolId(59L)
                        .plateId(2510L)
                        .measId(131L)
                        .build(),
                ResultSetDTO.builder()
                        .id(44L)
                        .executionStartTimeStamp(LocalDateTime.now().minusMinutes(10))
                        .outcome(StatusCode.SCHEDULED)
                        .protocolId(9L)
                        .plateId(10L)
                        .measId(11L)
                        .build()
        ));

        var f1 = new CompletableFuture<Long>();
        f1.complete(45L);
        var f2 = new CompletableFuture<ResultSetDTO>(); // result is ignored
        var f3 = new CompletableFuture<Long>();
        f3.complete(46L);
        var f4 = new CompletableFuture<ResultSetDTO>(); // result is ignored

        when(protocolExecutorService.execute(59L, 2510L, 131L)).thenReturn(new ProtocolExecutorService.ProtocolExecution(f1, f2));
        when(protocolExecutorService.execute(9L, 10L, 11L)).thenReturn(new ProtocolExecutorService.ProtocolExecution(f3, f4));

        calculationRecoveryService.recoverCalculations();

        verify(resultDataServiceClient).getResultSet(StatusCode.SCHEDULED);
        var errorCapture = ArgumentCaptor.forClass(List.class);
        verify(resultDataServiceClient).completeResultDataSet(eq(43L), eq(StatusCode.FAILURE), errorCapture.capture(), matches(" - Timestamp: \\[.*\\], Description: \\[Calculation re-scheduled because CalculationService was restarted\\], Id of new ResultSet: \\[45\\]"));
        verify(resultDataServiceClient).completeResultDataSet(eq(44L), eq(StatusCode.FAILURE), errorCapture.capture(), matches(" - Timestamp: \\[.*\\], Description: \\[Calculation re-scheduled because CalculationService was restarted\\], Id of new ResultSet: \\[46\\]"));
        Assertions.assertEquals(2, errorCapture.getAllValues().size());
        Assertions.assertEquals("Calculation re-scheduled because CalculationService was restarted", ((ErrorDTO) errorCapture.getAllValues().get(0).get(0)).getDescription());
        Assertions.assertEquals(45L, ((ErrorDTO) errorCapture.getAllValues().get(0).get(0)).getNewResultSetId());
        Assertions.assertEquals("Calculation re-scheduled because CalculationService was restarted", ((ErrorDTO) errorCapture.getAllValues().get(1).get(0)).getDescription());
        Assertions.assertEquals(46L, ((ErrorDTO) errorCapture.getAllValues().get(1).get(0)).getNewResultSetId());

    }

}
