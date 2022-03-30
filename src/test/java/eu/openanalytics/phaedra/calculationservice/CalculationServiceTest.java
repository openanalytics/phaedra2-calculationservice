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

import eu.openanalytics.phaedra.calculationservice.dto.CalculationStatus;
import eu.openanalytics.phaedra.calculationservice.enumeration.CalculationStatusCode;
import eu.openanalytics.phaedra.calculationservice.model.Feature;
import eu.openanalytics.phaedra.calculationservice.model.FeatureStat;
import eu.openanalytics.phaedra.calculationservice.model.Protocol;
import eu.openanalytics.phaedra.calculationservice.model.Sequence;
import eu.openanalytics.phaedra.calculationservice.service.ModelMapper;
import eu.openanalytics.phaedra.calculationservice.service.protocol.ProtocolInfoCollector;
import eu.openanalytics.phaedra.calculationservice.service.status.CalculationStatusService;
import eu.openanalytics.phaedra.plateservice.client.PlateServiceClient;
import eu.openanalytics.phaedra.plateservice.client.exception.PlateUnresolvableException;
import eu.openanalytics.phaedra.plateservice.dto.PlateDTO;
import eu.openanalytics.phaedra.protocolservice.client.exception.ProtocolUnresolvableException;
import eu.openanalytics.phaedra.resultdataservice.client.ResultDataServiceClient;
import eu.openanalytics.phaedra.resultdataservice.client.exception.ResultDataUnresolvableException;
import eu.openanalytics.phaedra.resultdataservice.client.exception.ResultFeatureStatUnresolvableException;
import eu.openanalytics.phaedra.resultdataservice.client.exception.ResultSetUnresolvableException;
import eu.openanalytics.phaedra.resultdataservice.dto.ResultDataDTO;
import eu.openanalytics.phaedra.resultdataservice.dto.ResultFeatureStatDTO;
import eu.openanalytics.phaedra.resultdataservice.dto.ResultSetDTO;
import eu.openanalytics.phaedra.resultdataservice.enumeration.StatusCode;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

public class CalculationServiceTest {

    private <T> T mockUnimplemented(Class<T> clazz) {
        return mock(clazz, invocation -> {
            throw new IllegalStateException(String.format("[%s:%s] must be stubbed with arguments [%s]!", invocation.getMock().getClass().getSimpleName(), invocation.getMethod().getName(), Arrays.toString(invocation.getArguments())));
        });
    }

    private ProtocolInfoCollector protocolInfoCollector;
    private PlateServiceClient plateServiceClient;
    private ResultDataServiceClient resultDataServiceClient;
    private CalculationStatusService calculationStatusService;
    private final ModelMapper modelMapper = new ModelMapper();

    @BeforeEach
    public void before() {
        protocolInfoCollector = mockUnimplemented(ProtocolInfoCollector.class);
        plateServiceClient = mockUnimplemented(PlateServiceClient.class);
        resultDataServiceClient = mockUnimplemented(ResultDataServiceClient.class);
        calculationStatusService = new CalculationStatusService(protocolInfoCollector, plateServiceClient, resultDataServiceClient, modelMapper);
    }

    @Test
    public void simpleSuccessfulCalculation() throws Exception {
        mockResultSet(StatusCode.SUCCESS);
        mockProtocol();
        mockPlate();
        mockResultData(
                ResultDataDTO.builder().id(1L).featureId(1L).statusCode(StatusCode.SUCCESS).build(),
                ResultDataDTO.builder().id(2L).featureId(2L).statusCode(StatusCode.SUCCESS).build(),
                ResultDataDTO.builder().id(3L).featureId(3L).statusCode(StatusCode.SUCCESS).build());

        mockFeatureStatData(
                // feature 1: count
                ResultFeatureStatDTO.builder().id(1L).featureId(1L).featureStatId(1L).welltype(null).statusCode(StatusCode.SUCCESS).build(),
                ResultFeatureStatDTO.builder().id(2L).featureId(1L).featureStatId(1L).welltype("LC").statusCode(StatusCode.SUCCESS).build(),
                ResultFeatureStatDTO.builder().id(3L).featureId(1L).featureStatId(1L).welltype("SAMPLE").statusCode(StatusCode.SUCCESS).build(),
                ResultFeatureStatDTO.builder().id(4L).featureId(1L).featureStatId(1L).welltype("HC").statusCode(StatusCode.SUCCESS).build(),
                // feature 1: zprime
                ResultFeatureStatDTO.builder().id(5L).featureId(1L).featureStatId(2L).welltype(null).statusCode(StatusCode.SUCCESS).build(),
                // feature 2: count
                ResultFeatureStatDTO.builder().id(6L).featureId(2L).featureStatId(3L).welltype(null).statusCode(StatusCode.SUCCESS).build(),
                ResultFeatureStatDTO.builder().id(7L).featureId(2L).featureStatId(3L).welltype("LC").statusCode(StatusCode.SUCCESS).build(),
                ResultFeatureStatDTO.builder().id(8L).featureId(2L).featureStatId(3L).welltype("SAMPLE").statusCode(StatusCode.SUCCESS).build(),
                ResultFeatureStatDTO.builder().id(9L).featureId(2L).featureStatId(3L).welltype("HC").statusCode(StatusCode.SUCCESS).build(),
                // feature 2: zprime
                ResultFeatureStatDTO.builder().id(10L).featureId(2L).featureStatId(4L).welltype(null).statusCode(StatusCode.SUCCESS).build(),
                // feature 3: min
                ResultFeatureStatDTO.builder().id(11L).featureId(3L).featureStatId(5L).welltype(null).statusCode(StatusCode.SUCCESS).build(),
                ResultFeatureStatDTO.builder().id(12L).featureId(3L).featureStatId(5L).welltype("LC").statusCode(StatusCode.SUCCESS).build(),
                ResultFeatureStatDTO.builder().id(13L).featureId(3L).featureStatId(5L).welltype("SAMPLE").statusCode(StatusCode.SUCCESS).build(),
                ResultFeatureStatDTO.builder().id(14L).featureId(3L).featureStatId(5L).welltype("HC").statusCode(StatusCode.SUCCESS).build(),
                // feature 3: max
                ResultFeatureStatDTO.builder().id(15L).featureId(3L).featureStatId(6L).welltype(null).statusCode(StatusCode.SUCCESS).build(),
                ResultFeatureStatDTO.builder().id(16L).featureId(3L).featureStatId(6L).welltype("LC").statusCode(StatusCode.SUCCESS).build(),
                ResultFeatureStatDTO.builder().id(17L).featureId(3L).featureStatId(6L).welltype("SAMPLE").statusCode(StatusCode.SUCCESS).build(),
                ResultFeatureStatDTO.builder().id(18L).featureId(3L).featureStatId(6L).welltype("HC").statusCode(StatusCode.SUCCESS).build(),
                // feature 3: zprime
                ResultFeatureStatDTO.builder().id(19L).featureId(3L).featureStatId(7L).welltype(null).statusCode(StatusCode.SUCCESS).build()
        );

        var status = calculationStatusService.getStatus(1);
        Assertions.assertEquals(StatusCode.SUCCESS, status.getStatusCode());

        // verify complexity
        Assertions.assertEquals(3, status.getComplexity().getFeatures());
        Assertions.assertEquals(10, status.getComplexity().getSteps());
        Assertions.assertEquals(7, status.getComplexity().getFeatureStats());
        Assertions.assertEquals(19, status.getComplexity().getFeatureStatResults());
        Assertions.assertEquals(2, status.getComplexity().getSequences());

        // verify sequence 0
        var sequence0 = status.getSequences().get(0);
        assertSuccessStatusCode(sequence0.getStatus());
        Assertions.assertEquals(2, sequence0.getFeatures().size());

        // verify feature 1
        var feature1 = sequence0.getFeatures().get(1L);
        assertSuccessStatusCode(feature1.getStatus());
        assertSuccessStatusCode(feature1.getStatStatus());
        Assertions.assertEquals(2, feature1.getStats().size());
        assertSuccessStatusCode(feature1.getStats().get(1L));
        assertSuccessStatusCode(feature1.getStats().get(2L));

        // verify feature
        var feature2 = sequence0.getFeatures().get(2L);
        assertSuccessStatusCode(feature2.getStatus());
        assertSuccessStatusCode(feature2.getStatStatus());
        Assertions.assertEquals(2, feature2.getStats().size());
        assertSuccessStatusCode(feature2.getStats().get(3L));
        assertSuccessStatusCode(feature2.getStats().get(4L));

        // verify sequence 1
        var sequence1 = status.getSequences().get(1);
        assertSuccessStatusCode(sequence1.getStatus());
        Assertions.assertEquals(1, sequence1.getFeatures().size());

        // verify feature 1
        var feature3 = sequence1.getFeatures().get(3L);
        assertSuccessStatusCode(feature3.getStatus());
        assertSuccessStatusCode(feature3.getStatStatus());
        Assertions.assertEquals(3, feature3.getStats().size());
        assertSuccessStatusCode(feature3.getStats().get(5L));
        assertSuccessStatusCode(feature3.getStats().get(6L));
        assertSuccessStatusCode(feature3.getStats().get(7L));
    }

    @Test
    public void featureFailedTest() throws Exception {
        mockResultSet(StatusCode.FAILURE);
        mockProtocol();
        mockPlate();
        mockResultData(
                ResultDataDTO.builder().id(1L).featureId(1L).statusCode(StatusCode.SUCCESS).build(),
                ResultDataDTO.builder().id(2L).featureId(2L).statusCode(StatusCode.FAILURE).statusMessage("Something went wrong during the calculation!").build()
        );

        mockFeatureStatData(
                // feature 1: count
                ResultFeatureStatDTO.builder().id(1L).featureId(1L).featureStatId(1L).welltype(null).statusCode(StatusCode.SUCCESS).build(),
                ResultFeatureStatDTO.builder().id(2L).featureId(1L).featureStatId(1L).welltype("LC").statusCode(StatusCode.SUCCESS).build(),
                ResultFeatureStatDTO.builder().id(3L).featureId(1L).featureStatId(1L).welltype("SAMPLE").statusCode(StatusCode.SUCCESS).build(),
                ResultFeatureStatDTO.builder().id(4L).featureId(1L).featureStatId(1L).welltype("HC").statusCode(StatusCode.SUCCESS).build(),
                // feature 1: zprime
                ResultFeatureStatDTO.builder().id(5L).featureId(1L).featureStatId(2L).welltype(null).statusCode(StatusCode.SUCCESS).build()
        );

        var status = calculationStatusService.getStatus(1);
        Assertions.assertEquals(StatusCode.FAILURE, status.getStatusCode());

        // verify complexity
        Assertions.assertEquals(3, status.getComplexity().getFeatures());
        Assertions.assertEquals(10, status.getComplexity().getSteps());
        Assertions.assertEquals(7, status.getComplexity().getFeatureStats());
        Assertions.assertEquals(19, status.getComplexity().getFeatureStatResults());
        Assertions.assertEquals(2, status.getComplexity().getSequences());

        // verify sequence 0
        var sequence0 = status.getSequences().get(0);
        Assertions.assertEquals(CalculationStatusCode.FAILURE, sequence0.getStatus().getStatusCode());
        Assertions.assertNull(sequence0.getStatus().getStatusMessage());
        Assertions.assertEquals("Sequence marked as failed because at least one feature failed.", sequence0.getStatus().getDescription());
        Assertions.assertEquals(2, sequence0.getFeatures().size());

        // verify feature 1
        var feature1 = sequence0.getFeatures().get(1L);
        assertSuccessStatusCode(feature1.getStatus());
        assertSuccessStatusCode(feature1.getStatStatus());
        Assertions.assertEquals(2, feature1.getStats().size());
        assertSuccessStatusCode(feature1.getStats().get(1L));
        assertSuccessStatusCode(feature1.getStats().get(2L));

        // verify feature
        var feature2 = sequence0.getFeatures().get(2L);
        Assertions.assertEquals(CalculationStatusCode.FAILURE, feature2.getStatus().getStatusCode());
        Assertions.assertEquals("Something went wrong during the calculation!", feature2.getStatus().getStatusMessage());
        Assertions.assertNull(feature2.getStatus().getDescription());
        Assertions.assertEquals(CalculationStatusCode.SKIPPED, feature2.getStatStatus().getStatusCode());
        Assertions.assertNull(feature2.getStatStatus().getStatusMessage());
        Assertions.assertNull(feature2.getStatStatus().getDescription());
        Assertions.assertEquals(2, feature2.getStats().size());
        Assertions.assertEquals(CalculationStatusCode.SKIPPED, feature2.getStats().get(3L).getStatusCode());
        Assertions.assertEquals("Skipped because calculating the corresponding feature failed.", feature2.getStats().get(3L).getDescription());
        Assertions.assertEquals(CalculationStatusCode.SKIPPED, feature2.getStats().get(4L).getStatusCode());
        Assertions.assertEquals("Skipped because calculating the corresponding feature failed.", feature2.getStats().get(4L).getDescription());

        // verify sequence 1
        var sequence1 = status.getSequences().get(1);
        Assertions.assertEquals(CalculationStatusCode.SKIPPED, sequence1.getStatus().getStatusCode());
        Assertions.assertNull(sequence1.getStatus().getStatusMessage());
        Assertions.assertNull( sequence1.getStatus().getDescription());
        Assertions.assertEquals(1, sequence1.getFeatures().size());

        // verify feature 1
        var feature3 = sequence1.getFeatures().get(3L);
        Assertions.assertEquals(CalculationStatusCode.SKIPPED, feature3.getStatus().getStatusCode());
        Assertions.assertEquals(CalculationStatusCode.SKIPPED, feature3.getStatStatus().getStatusCode());
        Assertions.assertEquals(3, feature3.getStats().size());
        Assertions.assertEquals(CalculationStatusCode.SKIPPED, feature3.getStats().get(5L).getStatusCode());
        Assertions.assertEquals(CalculationStatusCode.SKIPPED, feature3.getStats().get(6L).getStatusCode());
        Assertions.assertEquals(CalculationStatusCode.SKIPPED, feature3.getStats().get(7L).getStatusCode());
    }

    @Test
    public void featureStatFailed() throws Exception {
        mockResultSet(StatusCode.FAILURE);
        mockProtocol();
        mockPlate();
        mockResultData(
                ResultDataDTO.builder().id(1L).featureId(1L).statusCode(StatusCode.SUCCESS).build(),
                ResultDataDTO.builder().id(2L).featureId(2L).statusCode(StatusCode.SUCCESS).build(),
                ResultDataDTO.builder().id(3L).featureId(3L).statusCode(StatusCode.SUCCESS).build());

        mockFeatureStatData(
                // feature 1: count
                ResultFeatureStatDTO.builder().id(1L).featureId(1L).featureStatId(1L).welltype(null).statusCode(StatusCode.SUCCESS).build(),
                ResultFeatureStatDTO.builder().id(2L).featureId(1L).featureStatId(1L).welltype("LC").statusCode(StatusCode.SUCCESS).build(),
                ResultFeatureStatDTO.builder().id(3L).featureId(1L).featureStatId(1L).welltype("SAMPLE").statusCode(StatusCode.SUCCESS).build(),
                ResultFeatureStatDTO.builder().id(4L).featureId(1L).featureStatId(1L).welltype("HC").statusCode(StatusCode.SUCCESS).build(),
                // feature 1: zprime
                ResultFeatureStatDTO.builder().id(5L).featureId(1L).featureStatId(2L).welltype(null).statusCode(StatusCode.SUCCESS).build(),
                // feature 2: count
                ResultFeatureStatDTO.builder().id(6L).featureId(2L).featureStatId(3L).welltype(null).statusCode(StatusCode.SUCCESS).build(),
                ResultFeatureStatDTO.builder().id(7L).featureId(2L).featureStatId(3L).welltype("LC").statusCode(StatusCode.SUCCESS).build(),
                ResultFeatureStatDTO.builder().id(8L).featureId(2L).featureStatId(3L).welltype("SAMPLE").statusCode(StatusCode.SUCCESS).build(),
                ResultFeatureStatDTO.builder().id(9L).featureId(2L).featureStatId(3L).welltype("HC").statusCode(StatusCode.SUCCESS).build(),
                // feature 2: zprime
                ResultFeatureStatDTO.builder().id(10L).featureId(2L).featureStatId(4L).welltype(null).statusCode(StatusCode.FAILURE).statusMessage("Out of memory exception").build(),
                // feature 3: min
                ResultFeatureStatDTO.builder().id(11L).featureId(3L).featureStatId(5L).welltype(null).statusCode(StatusCode.SUCCESS).build(),
                ResultFeatureStatDTO.builder().id(12L).featureId(3L).featureStatId(5L).welltype("LC").statusCode(StatusCode.SUCCESS).build(),
                ResultFeatureStatDTO.builder().id(13L).featureId(3L).featureStatId(5L).welltype("SAMPLE").statusCode(StatusCode.SUCCESS).build(),
                ResultFeatureStatDTO.builder().id(14L).featureId(3L).featureStatId(5L).welltype("HC").statusCode(StatusCode.SUCCESS).build(),
                // feature 3: max
                ResultFeatureStatDTO.builder().id(15L).featureId(3L).featureStatId(6L).welltype(null).statusCode(StatusCode.SUCCESS).build(),
                ResultFeatureStatDTO.builder().id(16L).featureId(3L).featureStatId(6L).welltype("LC").statusCode(StatusCode.SUCCESS).build(),
                ResultFeatureStatDTO.builder().id(17L).featureId(3L).featureStatId(6L).welltype("SAMPLE").statusCode(StatusCode.SUCCESS).build(),
                ResultFeatureStatDTO.builder().id(18L).featureId(3L).featureStatId(6L).welltype("HC").statusCode(StatusCode.SUCCESS).build(),
                // feature 3: zprime
                ResultFeatureStatDTO.builder().id(19L).featureId(3L).featureStatId(7L).welltype(null).statusCode(StatusCode.FAILURE).statusMessage("Out of memory exception").build()
        );

        var status = calculationStatusService.getStatus(1);
        Assertions.assertEquals(StatusCode.FAILURE, status.getStatusCode());

        // verify complexity
        Assertions.assertEquals(3, status.getComplexity().getFeatures());
        Assertions.assertEquals(10, status.getComplexity().getSteps());
        Assertions.assertEquals(7, status.getComplexity().getFeatureStats());
        Assertions.assertEquals(19, status.getComplexity().getFeatureStatResults());
        Assertions.assertEquals(2, status.getComplexity().getSequences());

        // verify sequence 0
        var sequence0 = status.getSequences().get(0);
        Assertions.assertEquals(CalculationStatusCode.FAILURE, sequence0.getStatus().getStatusCode());
        Assertions.assertNull(sequence0.getStatus().getStatusMessage());
        Assertions.assertEquals("Sequence marked as failed because at least one featureStat failed (next sequence will still be calculated).", sequence0.getStatus().getDescription());
        Assertions.assertEquals(2, sequence0.getFeatures().size());

        // verify feature 1
        var feature1 = sequence0.getFeatures().get(1L);
        assertSuccessStatusCode(feature1.getStatus());
        assertSuccessStatusCode(feature1.getStatStatus());
        Assertions.assertEquals(2, feature1.getStats().size());
        assertSuccessStatusCode(feature1.getStats().get(1L));
        assertSuccessStatusCode(feature1.getStats().get(2L));

        // verify feature
        var feature2 = sequence0.getFeatures().get(2L);
        assertSuccessStatusCode(feature2.getStatus());
        Assertions.assertEquals(CalculationStatusCode.FAILURE, feature2.getStatStatus().getStatusCode());
        Assertions.assertNull(feature2.getStatStatus().getStatusMessage());
        Assertions.assertNull(feature2.getStatStatus().getDescription());
        Assertions.assertEquals(2, feature2.getStats().size());
        assertSuccessStatusCode(feature2.getStats().get(3L));
        Assertions.assertEquals(CalculationStatusCode.FAILURE, feature2.getStats().get(4L).getStatusCode());
        Assertions.assertEquals("Out of memory exception", feature2.getStats().get(4L).getStatusMessage());
        Assertions.assertNull(feature2.getStats().get(4L).getDescription());

        // verify sequence 1
        var sequence1 = status.getSequences().get(1);
        Assertions.assertEquals(CalculationStatusCode.FAILURE, sequence0.getStatus().getStatusCode());
        Assertions.assertNull(sequence0.getStatus().getStatusMessage());
        Assertions.assertEquals("Sequence marked as failed because at least one featureStat failed (next sequence will still be calculated).", sequence0.getStatus().getDescription());
        Assertions.assertEquals(1, sequence1.getFeatures().size());

        // verify feature 1
        var feature3 = sequence1.getFeatures().get(3L);
        assertSuccessStatusCode(feature3.getStatus());
        Assertions.assertEquals(CalculationStatusCode.FAILURE, feature2.getStatStatus().getStatusCode());
        Assertions.assertNull(feature2.getStatStatus().getStatusMessage());
        Assertions.assertNull(feature2.getStatStatus().getDescription());
        Assertions.assertEquals(3, feature3.getStats().size());
        assertSuccessStatusCode(feature3.getStats().get(5L));
        assertSuccessStatusCode(feature3.getStats().get(6L));
        Assertions.assertEquals(CalculationStatusCode.FAILURE, feature3.getStats().get(7L).getStatusCode());
        Assertions.assertEquals("Out of memory exception", feature3.getStats().get(7L).getStatusMessage());
        Assertions.assertNull(feature3.getStats().get(7L).getDescription());
    }

    @Test
    public void featureStillRunning() throws Exception {
        mockResultSet(StatusCode.SCHEDULED);
        mockProtocol();
        mockPlate();
        mockResultData(
                ResultDataDTO.builder().id(1L).featureId(1L).statusCode(StatusCode.SUCCESS).build(),
                ResultDataDTO.builder().id(2L).featureId(2L).statusCode(StatusCode.SUCCESS).build());

        mockFeatureStatData(
                // feature 1: count
                ResultFeatureStatDTO.builder().id(1L).featureId(1L).featureStatId(1L).welltype(null).statusCode(StatusCode.SUCCESS).build(),
                ResultFeatureStatDTO.builder().id(2L).featureId(1L).featureStatId(1L).welltype("LC").statusCode(StatusCode.SUCCESS).build(),
                ResultFeatureStatDTO.builder().id(3L).featureId(1L).featureStatId(1L).welltype("SAMPLE").statusCode(StatusCode.SUCCESS).build(),
                ResultFeatureStatDTO.builder().id(4L).featureId(1L).featureStatId(1L).welltype("HC").statusCode(StatusCode.SUCCESS).build(),
                // feature 1: zprime
                ResultFeatureStatDTO.builder().id(5L).featureId(1L).featureStatId(2L).welltype(null).statusCode(StatusCode.SUCCESS).build(),
                // feature 2: count
                ResultFeatureStatDTO.builder().id(6L).featureId(2L).featureStatId(3L).welltype(null).statusCode(StatusCode.SUCCESS).build(),
                ResultFeatureStatDTO.builder().id(7L).featureId(2L).featureStatId(3L).welltype("LC").statusCode(StatusCode.SUCCESS).build(),
                ResultFeatureStatDTO.builder().id(8L).featureId(2L).featureStatId(3L).welltype("SAMPLE").statusCode(StatusCode.SUCCESS).build(),
                ResultFeatureStatDTO.builder().id(9L).featureId(2L).featureStatId(3L).welltype("HC").statusCode(StatusCode.SUCCESS).build(),
                // feature 2: zprime
                ResultFeatureStatDTO.builder().id(10L).featureId(2L).featureStatId(4L).welltype(null).statusCode(StatusCode.SUCCESS).build()
        );

        var status = calculationStatusService.getStatus(1);
        Assertions.assertEquals(StatusCode.SCHEDULED, status.getStatusCode());

        // verify complexity
        Assertions.assertEquals(3, status.getComplexity().getFeatures());
        Assertions.assertEquals(10, status.getComplexity().getSteps());
        Assertions.assertEquals(7, status.getComplexity().getFeatureStats());
        Assertions.assertEquals(19, status.getComplexity().getFeatureStatResults());
        Assertions.assertEquals(2, status.getComplexity().getSequences());

        // verify sequence 0
        var sequence0 = status.getSequences().get(0);
        assertSuccessStatusCode(sequence0.getStatus());
        Assertions.assertEquals(2, sequence0.getFeatures().size());

        // verify feature 1
        var feature1 = sequence0.getFeatures().get(1L);
        assertSuccessStatusCode(feature1.getStatus());
        assertSuccessStatusCode(feature1.getStatStatus());
        Assertions.assertEquals(2, feature1.getStats().size());
        assertSuccessStatusCode(feature1.getStats().get(1L));
        assertSuccessStatusCode(feature1.getStats().get(2L));

        // verify feature
        var feature2 = sequence0.getFeatures().get(2L);
        assertSuccessStatusCode(feature2.getStatus());
        assertSuccessStatusCode(feature2.getStatStatus());
        Assertions.assertEquals(2, feature2.getStats().size());
        assertSuccessStatusCode(feature2.getStats().get(3L));
        assertSuccessStatusCode(feature2.getStats().get(4L));

        // verify sequence 1
        var sequence1 = status.getSequences().get(1);
        assertScheduledStatusCode(sequence1.getStatus());
        Assertions.assertEquals(1, sequence1.getFeatures().size());

        // verify feature 1
        var feature3 = sequence1.getFeatures().get(3L);
        assertScheduledStatusCode(feature3.getStatus());
        assertScheduledStatusCode(feature3.getStatStatus());
        Assertions.assertEquals(3, feature3.getStats().size());
        assertScheduledStatusCode(feature3.getStats().get(5L));
        assertScheduledStatusCode(feature3.getStats().get(6L));
        assertScheduledStatusCode(feature3.getStats().get(7L));
    }

    @Test
    public void featureStatStillRunning() throws Exception {
        mockResultSet(StatusCode.SCHEDULED);
        mockProtocol();
        mockPlate();
        mockResultData(
                ResultDataDTO.builder().id(1L).featureId(1L).statusCode(StatusCode.SUCCESS).build(),
                ResultDataDTO.builder().id(2L).featureId(2L).statusCode(StatusCode.SUCCESS).build(),
                ResultDataDTO.builder().id(3L).featureId(3L).statusCode(StatusCode.SUCCESS).build());

        mockFeatureStatData(
                // feature 1: count
                ResultFeatureStatDTO.builder().id(1L).featureId(1L).featureStatId(1L).welltype(null).statusCode(StatusCode.SUCCESS).build(),
                ResultFeatureStatDTO.builder().id(2L).featureId(1L).featureStatId(1L).welltype("LC").statusCode(StatusCode.SUCCESS).build(),
                ResultFeatureStatDTO.builder().id(3L).featureId(1L).featureStatId(1L).welltype("SAMPLE").statusCode(StatusCode.SUCCESS).build(),
                ResultFeatureStatDTO.builder().id(4L).featureId(1L).featureStatId(1L).welltype("HC").statusCode(StatusCode.SUCCESS).build(),
                // feature 1: zprime
                ResultFeatureStatDTO.builder().id(5L).featureId(1L).featureStatId(2L).welltype(null).statusCode(StatusCode.SUCCESS).build(),
                // feature 2: count
                ResultFeatureStatDTO.builder().id(6L).featureId(2L).featureStatId(3L).welltype(null).statusCode(StatusCode.SUCCESS).build(),
                ResultFeatureStatDTO.builder().id(7L).featureId(2L).featureStatId(3L).welltype("LC").statusCode(StatusCode.SUCCESS).build(),
                ResultFeatureStatDTO.builder().id(8L).featureId(2L).featureStatId(3L).welltype("SAMPLE").statusCode(StatusCode.SUCCESS).build(),
                ResultFeatureStatDTO.builder().id(9L).featureId(2L).featureStatId(3L).welltype("HC").statusCode(StatusCode.SUCCESS).build(),
                // feature 2: zprime -> still in progress
                // feature 3: min
                ResultFeatureStatDTO.builder().id(11L).featureId(3L).featureStatId(5L).welltype(null).statusCode(StatusCode.SUCCESS).build(),
                ResultFeatureStatDTO.builder().id(12L).featureId(3L).featureStatId(5L).welltype("LC").statusCode(StatusCode.SUCCESS).build(),
                ResultFeatureStatDTO.builder().id(13L).featureId(3L).featureStatId(5L).welltype("SAMPLE").statusCode(StatusCode.SUCCESS).build(),
                ResultFeatureStatDTO.builder().id(14L).featureId(3L).featureStatId(5L).welltype("HC").statusCode(StatusCode.SUCCESS).build(),
                // feature 3: max
                ResultFeatureStatDTO.builder().id(15L).featureId(3L).featureStatId(6L).welltype(null).statusCode(StatusCode.SUCCESS).build(),
                ResultFeatureStatDTO.builder().id(16L).featureId(3L).featureStatId(6L).welltype("LC").statusCode(StatusCode.SUCCESS).build(),
                ResultFeatureStatDTO.builder().id(17L).featureId(3L).featureStatId(6L).welltype("SAMPLE").statusCode(StatusCode.SUCCESS).build(),
                ResultFeatureStatDTO.builder().id(18L).featureId(3L).featureStatId(6L).welltype("HC").statusCode(StatusCode.SUCCESS).build(),
                // feature 3: zprime
                ResultFeatureStatDTO.builder().id(19L).featureId(3L).featureStatId(7L).welltype(null).statusCode(StatusCode.SUCCESS).build()
        );

        var status = calculationStatusService.getStatus(1);
        Assertions.assertEquals(StatusCode.SCHEDULED, status.getStatusCode());

        // verify complexity
        Assertions.assertEquals(3, status.getComplexity().getFeatures());
        Assertions.assertEquals(10, status.getComplexity().getSteps());
        Assertions.assertEquals(7, status.getComplexity().getFeatureStats());
        Assertions.assertEquals(19, status.getComplexity().getFeatureStatResults());
        Assertions.assertEquals(2, status.getComplexity().getSequences());

        // verify sequence 0
        var sequence0 = status.getSequences().get(0);
        assertScheduledStatusCode(sequence0.getStatus());
        Assertions.assertEquals(2, sequence0.getFeatures().size());

        // verify feature 1
        var feature1 = sequence0.getFeatures().get(1L);
        assertSuccessStatusCode(feature1.getStatus());
        assertSuccessStatusCode(feature1.getStatStatus());
        Assertions.assertEquals(2, feature1.getStats().size());
        assertSuccessStatusCode(feature1.getStats().get(1L));
        assertSuccessStatusCode(feature1.getStats().get(2L));

        // verify feature
        var feature2 = sequence0.getFeatures().get(2L);
        assertSuccessStatusCode(feature2.getStatus());
        assertScheduledStatusCode(feature2.getStatStatus());
        Assertions.assertEquals(2, feature2.getStats().size());
        assertSuccessStatusCode(feature2.getStats().get(3L));
        assertScheduledStatusCode(feature2.getStats().get(4L));

        // verify sequence 1
        var sequence1 = status.getSequences().get(1);
        assertSuccessStatusCode(sequence1.getStatus());
        Assertions.assertEquals(1, sequence1.getFeatures().size());

        // verify feature 1
        var feature3 = sequence1.getFeatures().get(3L);
        assertSuccessStatusCode(feature3.getStatus());
        assertSuccessStatusCode(feature3.getStatStatus());
        Assertions.assertEquals(3, feature3.getStats().size());
        assertSuccessStatusCode(feature3.getStats().get(5L));
        assertSuccessStatusCode(feature3.getStats().get(6L));
        assertSuccessStatusCode(feature3.getStats().get(7L));
    }

    @Test
    public void stillNeedToStart() throws Exception{
        mockResultSet(StatusCode.SCHEDULED);
        mockProtocol();
        mockPlate();
        mockResultData(); // still need to start -> no data
        mockFeatureStatData(); // still need to start -> no data

        var status = calculationStatusService.getStatus(1);
        Assertions.assertEquals(StatusCode.SCHEDULED, status.getStatusCode());

        // verify complexity
        Assertions.assertEquals(3, status.getComplexity().getFeatures());
        Assertions.assertEquals(10, status.getComplexity().getSteps());
        Assertions.assertEquals(7, status.getComplexity().getFeatureStats());
        Assertions.assertEquals(19, status.getComplexity().getFeatureStatResults());
        Assertions.assertEquals(2, status.getComplexity().getSequences());

        // verify sequence 0
        var sequence0 = status.getSequences().get(0);
        assertScheduledStatusCode(sequence0.getStatus());
        Assertions.assertEquals(2, sequence0.getFeatures().size());

        // verify feature 1
        var feature1 = sequence0.getFeatures().get(1L);
        assertScheduledStatusCode(feature1.getStatus());
        assertScheduledStatusCode(feature1.getStatStatus());
        Assertions.assertEquals(2, feature1.getStats().size());
        assertScheduledStatusCode(feature1.getStats().get(1L));
        assertScheduledStatusCode(feature1.getStats().get(2L));

        // verify feature
        var feature2 = sequence0.getFeatures().get(2L);
        assertScheduledStatusCode(feature2.getStatus());
        assertScheduledStatusCode(feature2.getStatStatus());
        Assertions.assertEquals(2, feature2.getStats().size());
        assertScheduledStatusCode(feature2.getStats().get(3L));
        assertScheduledStatusCode(feature2.getStats().get(4L));

        // verify sequence 1
        var sequence1 = status.getSequences().get(1);
        assertScheduledStatusCode(sequence1.getStatus());
        Assertions.assertEquals(1, sequence1.getFeatures().size());

        // verify feature 1
        var feature3 = sequence1.getFeatures().get(3L);
        assertScheduledStatusCode(feature3.getStatus());
        assertScheduledStatusCode(feature3.getStatStatus());
        Assertions.assertEquals(3, feature3.getStats().size());
        assertScheduledStatusCode(feature3.getStats().get(5L));
        assertScheduledStatusCode(feature3.getStats().get(6L));
        assertScheduledStatusCode(feature3.getStats().get(7L));
    }

    @Test
    public void notYetAllResultsForSingleFeatureStat() throws Exception {
        mockResultSet(StatusCode.SCHEDULED);
        mockProtocol();
        mockPlate();
        mockResultData(
                ResultDataDTO.builder().id(1L).featureId(1L).statusCode(StatusCode.SUCCESS).build(),
                ResultDataDTO.builder().id(2L).featureId(2L).statusCode(StatusCode.SUCCESS).build(),
                ResultDataDTO.builder().id(3L).featureId(3L).statusCode(StatusCode.SUCCESS).build());

        mockFeatureStatData(
                // feature 1: count
                ResultFeatureStatDTO.builder().id(1L).featureId(1L).featureStatId(1L).welltype(null).statusCode(StatusCode.SUCCESS).build(),
                ResultFeatureStatDTO.builder().id(2L).featureId(1L).featureStatId(1L).welltype("LC").statusCode(StatusCode.SUCCESS).build(),
                ResultFeatureStatDTO.builder().id(3L).featureId(1L).featureStatId(1L).welltype("SAMPLE").statusCode(StatusCode.SUCCESS).build(),
                ResultFeatureStatDTO.builder().id(4L).featureId(1L).featureStatId(1L).welltype("HC").statusCode(StatusCode.SUCCESS).build(),
                // feature 1: zprime
                ResultFeatureStatDTO.builder().id(5L).featureId(1L).featureStatId(2L).welltype(null).statusCode(StatusCode.SUCCESS).build(),
                // feature 2: count
                ResultFeatureStatDTO.builder().id(6L).featureId(2L).featureStatId(3L).welltype(null).statusCode(StatusCode.SUCCESS).build(),
                ResultFeatureStatDTO.builder().id(7L).featureId(2L).featureStatId(3L).welltype("LC").statusCode(StatusCode.SUCCESS).build(),
                ResultFeatureStatDTO.builder().id(8L).featureId(2L).featureStatId(3L).welltype("SAMPLE").statusCode(StatusCode.SUCCESS).build(),
                ResultFeatureStatDTO.builder().id(9L).featureId(2L).featureStatId(3L).welltype("HC").statusCode(StatusCode.SUCCESS).build(),
                // feature 2: zprime
                ResultFeatureStatDTO.builder().id(10L).featureId(2L).featureStatId(4L).welltype(null).statusCode(StatusCode.SUCCESS).build(),
                // feature 3: min
                ResultFeatureStatDTO.builder().id(11L).featureId(3L).featureStatId(5L).welltype(null).statusCode(StatusCode.SUCCESS).build(),
                ResultFeatureStatDTO.builder().id(12L).featureId(3L).featureStatId(5L).welltype("LC").statusCode(StatusCode.SUCCESS).build(),
                ResultFeatureStatDTO.builder().id(13L).featureId(3L).featureStatId(5L).welltype("SAMPLE").statusCode(StatusCode.SUCCESS).build(),
                ResultFeatureStatDTO.builder().id(14L).featureId(3L).featureStatId(5L).welltype("HC").statusCode(StatusCode.SUCCESS).build(),
                // feature 3: max -> does not yet contain the value for the HC welltype
                ResultFeatureStatDTO.builder().id(15L).featureId(3L).featureStatId(6L).welltype(null).statusCode(StatusCode.SUCCESS).build(),
                ResultFeatureStatDTO.builder().id(16L).featureId(3L).featureStatId(6L).welltype("LC").statusCode(StatusCode.SUCCESS).build(),
                ResultFeatureStatDTO.builder().id(17L).featureId(3L).featureStatId(6L).welltype("SAMPLE").statusCode(StatusCode.SUCCESS).build(),
//                ResultFeatureStatDTO.builder().id(18L).featureId(3L).featureStatId(6L).welltype("HC").statusCode(StatusCode.SUCCESS).build(),
                // feature 3: zprime
                ResultFeatureStatDTO.builder().id(19L).featureId(3L).featureStatId(7L).welltype(null).statusCode(StatusCode.SUCCESS).build()
        );

        var status = calculationStatusService.getStatus(1);
        Assertions.assertEquals(StatusCode.SCHEDULED, status.getStatusCode());

        // verify complexity
        Assertions.assertEquals(3, status.getComplexity().getFeatures());
        Assertions.assertEquals(10, status.getComplexity().getSteps());
        Assertions.assertEquals(7, status.getComplexity().getFeatureStats());
        Assertions.assertEquals(19, status.getComplexity().getFeatureStatResults());
        Assertions.assertEquals(2, status.getComplexity().getSequences());

        // verify sequence 0
        var sequence0 = status.getSequences().get(0);
        assertSuccessStatusCode(sequence0.getStatus());
        Assertions.assertEquals(2, sequence0.getFeatures().size());

        // verify feature 1
        var feature1 = sequence0.getFeatures().get(1L);
        assertSuccessStatusCode(feature1.getStatus());
        assertSuccessStatusCode(feature1.getStatStatus());
        Assertions.assertEquals(2, feature1.getStats().size());
        assertSuccessStatusCode(feature1.getStats().get(1L));
        assertSuccessStatusCode(feature1.getStats().get(2L));

        // verify feature
        var feature2 = sequence0.getFeatures().get(2L);
        assertSuccessStatusCode(feature2.getStatus());
        assertSuccessStatusCode(feature2.getStatStatus());
        Assertions.assertEquals(2, feature2.getStats().size());
        assertSuccessStatusCode(feature2.getStats().get(3L));
        assertSuccessStatusCode(feature2.getStats().get(4L));

        // verify sequence 1
        var sequence1 = status.getSequences().get(1);
        assertScheduledStatusCode(sequence1.getStatus());
        Assertions.assertEquals(1, sequence1.getFeatures().size());

        // verify feature 1
        var feature3 = sequence1.getFeatures().get(3L);
        assertSuccessStatusCode(feature3.getStatus());
        assertScheduledStatusCode(feature3.getStatStatus());
        Assertions.assertEquals(3, feature3.getStats().size());
        assertSuccessStatusCode(feature3.getStats().get(5L));
        assertScheduledStatusCode(feature3.getStats().get(6L));
        assertSuccessStatusCode(feature3.getStats().get(7L));
    }

    private void mockFeatureStatData(ResultFeatureStatDTO... data) throws ResultFeatureStatUnresolvableException {
        doReturn(Arrays.asList(data)).when(resultDataServiceClient).getResultFeatureStat(1);
    }

    private void mockResultData(ResultDataDTO... data) throws ResultDataUnresolvableException {
        doReturn(Arrays.asList(data)).when(resultDataServiceClient).getResultData(1);
    }

    private void mockPlate() throws PlateUnresolvableException {
        doReturn(PlateDTO.builder().id(1L).build()).when(plateServiceClient).getPlate(1);
    }

    private void mockProtocol() throws ProtocolUnresolvableException {
        doReturn(Protocol.builder()
                .id(1L)
                .sequences(new HashMap<>() {{
                    put(0, new Sequence(0, List.of(
                            Feature.builder()
                                    .id(1L)
                                    .featureStats(List.of(
                                            FeatureStat.builder()
                                                    .id(1L)
                                                    .plateStat(true)
                                                    .welltypeStat(true)
                                                    .name("count")
                                                    .build(),
                                            FeatureStat.builder()
                                                    .id(2L)
                                                    .plateStat(true)
                                                    .welltypeStat(false)
                                                    .name("zprime")
                                                    .build()
                                    ))
                                    .build(),
                            Feature.builder()
                                    .id(2L)
                                    .featureStats(List.of(
                                            FeatureStat.builder()
                                                    .id(3L)
                                                    .plateStat(true)
                                                    .welltypeStat(true)
                                                    .name("count")
                                                    .build(),
                                            FeatureStat.builder()
                                                    .id(4L)
                                                    .plateStat(true)
                                                    .welltypeStat(false)
                                                    .name("zprime")
                                                    .build()
                                    ))
                                    .build()
                    )));
                    put(1, new Sequence(1, List.of(
                            Feature.builder()
                                    .id(3L)
                                    .featureStats(List.of(
                                            FeatureStat.builder()
                                                    .id(5L)
                                                    .plateStat(true)
                                                    .welltypeStat(true)
                                                    .name("min")
                                                    .build(),
                                            FeatureStat.builder()
                                                    .id(6L)
                                                    .plateStat(true)
                                                    .welltypeStat(true)
                                                    .name("max")
                                                    .build(),
                                            FeatureStat.builder()
                                                    .id(7L)
                                                    .plateStat(true)
                                                    .welltypeStat(false)
                                                    .name("zprime")
                                                    .build()
                                    ))
                                    .build()
                    )));
                }})
                .build()).when(protocolInfoCollector).getProtocol(1);
    }

    private void assertSuccessStatusCode(CalculationStatus.StatusDescription desc) {
        Assertions.assertEquals(CalculationStatusCode.SUCCESS, desc.getStatusCode());
        Assertions.assertNull(desc.getStatusMessage());
        Assertions.assertNull(desc.getDescription());
    }

    private void assertScheduledStatusCode(CalculationStatus.StatusDescription desc) {
        Assertions.assertEquals(CalculationStatusCode.SCHEDULED, desc.getStatusCode());
        Assertions.assertNull(desc.getStatusMessage());
        Assertions.assertNull(desc.getDescription());
    }

    private void mockResultSet(StatusCode statusCode) throws ResultSetUnresolvableException {
        doReturn(ResultSetDTO.builder()
                .id(1L)
                .measId(1L)
                .plateId(1L)
                .protocolId(1L)
                .outcome(statusCode)
                .executionStartTimeStamp(LocalDateTime.now())
                .executionEndTimeStamp(LocalDateTime.now())
                .build()).when(resultDataServiceClient).getResultSet(1);
    }


}
