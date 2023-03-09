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
package eu.openanalytics.phaedra.calculationservice;

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.CompletableFuture;

import eu.openanalytics.phaedra.commons.dto.CalculationRequestDTO;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;

import eu.openanalytics.phaedra.calculationservice.api.CalculationController;
import eu.openanalytics.phaedra.calculationservice.dto.CalculationStatus;
import eu.openanalytics.phaedra.calculationservice.service.CalculationStatusService;
import eu.openanalytics.phaedra.calculationservice.service.protocol.ProtocolExecutorService;
import eu.openanalytics.phaedra.calculationservice.support.AbstractIntegrationTest;
import eu.openanalytics.phaedra.resultdataservice.dto.ResultSetDTO;
import eu.openanalytics.phaedra.resultdataservice.enumeration.StatusCode;
import org.springframework.kafka.core.KafkaTemplate;

@Disabled
public class CalculationIntegrationTest extends AbstractIntegrationTest {

    private static final ProtocolExecutorService protocolExecutorService = mock(ProtocolExecutorService.class);
    private static final CalculationStatusService calculationStatusService = mock(CalculationStatusService.class);

    @Test
    public void calculateTestNoTimeout() throws Exception {
        var f1 = new CompletableFuture<Long>();
        f1.complete(45L);
        var f2 = new CompletableFuture<ResultSetDTO>();
        f2.complete(new ResultSetDTO(45L, 1L, 1L, 1L, LocalDateTime.now(), LocalDateTime.now(), StatusCode.SUCCESS, new ArrayList<>(),"error"));
        when(protocolExecutorService.execute(anyLong(),anyLong(),anyLong())).thenReturn(new ProtocolExecutorService.ProtocolExecution(f1, f2));
        var calculationController = new CalculationController(protocolExecutorService, calculationStatusService);
        var calculationRequestDTO = CalculationRequestDTO.builder().protocolId(1L).plateId(1L).measId(1L).build();
        var res = calculationController.calculate(calculationRequestDTO, 1000L);

        Assertions.assertEquals(45L, res.getBody());
        Assertions.assertEquals(HttpStatus.CREATED, res.getStatusCode());
    }

    @Test
    public void calculateTestWithTimeout() throws Exception {
        var f1 = new CompletableFuture<Long>();
        f1.complete(45L);
        var f2 = new CompletableFuture<ResultSetDTO>();
        f2.complete(new ResultSetDTO(45L, 1L, 1L, 1L, LocalDateTime.now(), LocalDateTime.now(), StatusCode.SUCCESS, new ArrayList<>(),"error"));
        when(protocolExecutorService.execute(anyLong(),anyLong(),anyLong())).thenReturn(new ProtocolExecutorService.ProtocolExecution(f1, f2));
        var calculationController = new CalculationController(protocolExecutorService, calculationStatusService);
        var calculationRequestDTO = CalculationRequestDTO.builder().protocolId(1L).plateId(1L).measId(1L).build();

        var res = calculationController.calculate(calculationRequestDTO, 1000L);

        Assertions.assertEquals(45L,res.getBody());
        Assertions.assertEquals(HttpStatus.CREATED,res.getStatusCode());
    }

    @Test
    public void statusTestSuccess() throws Exception {
        when(calculationStatusService.getStatus(20L)).thenReturn(new CalculationStatus(new CalculationStatus.CalculationComplexityDTO(1,1,1,1,1),StatusCode.SUCCESS,new ArrayList<>(), new HashMap<>()));
        var calculationController = new CalculationController(protocolExecutorService, calculationStatusService);
        var res = calculationController.status(20);
        Assertions.assertEquals(new CalculationStatus(new CalculationStatus.CalculationComplexityDTO(1,1,1,1,1),StatusCode.SUCCESS,new ArrayList<>(), new HashMap<>()),res.getBody());
        Assertions.assertEquals(HttpStatus.OK, res.getStatusCode());
    }

    @Test
    public void statusTestScheduled() throws Exception {
        when(calculationStatusService.getStatus(20L)).thenReturn(new CalculationStatus(new CalculationStatus.CalculationComplexityDTO(1,1,1,1,1),StatusCode.SCHEDULED,new ArrayList<>(), new HashMap<>()));
        var calculationController = new CalculationController(protocolExecutorService, calculationStatusService);
        var res = calculationController.status(20);
        Assertions.assertEquals(new CalculationStatus(new CalculationStatus.CalculationComplexityDTO(1,1,1,1,1),StatusCode.SCHEDULED,new ArrayList<>(), new HashMap<>()),res.getBody());
        Assertions.assertEquals(HttpStatus.OK, res.getStatusCode());
    }

    @Test
    public void statusTestFailure() throws Exception {
        when(calculationStatusService.getStatus(20L)).thenReturn(new CalculationStatus(new CalculationStatus.CalculationComplexityDTO(1,1,1,1,1),StatusCode.FAILURE,new ArrayList<>(), new HashMap<>()));
        var calculationController = new CalculationController(protocolExecutorService, calculationStatusService);
        var res = calculationController.status(20);
        Assertions.assertEquals(new CalculationStatus(new CalculationStatus.CalculationComplexityDTO(1,1,1,1,1),StatusCode.FAILURE,new ArrayList<>(), new HashMap<>()),res.getBody());
        Assertions.assertEquals(HttpStatus.OK, res.getStatusCode());
    }
}
