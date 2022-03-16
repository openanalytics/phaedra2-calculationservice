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


import static eu.openanalytics.phaedra.calculationservice.CalculationService.R_FAST_LANE;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CompletableFuture;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;

import eu.openanalytics.phaedra.calculationservice.enumeration.CalculationScope;
import eu.openanalytics.phaedra.calculationservice.enumeration.Category;
import eu.openanalytics.phaedra.calculationservice.enumeration.FeatureType;
import eu.openanalytics.phaedra.calculationservice.enumeration.ScriptLanguage;
import eu.openanalytics.phaedra.calculationservice.model.CalculationInputValue;
import eu.openanalytics.phaedra.calculationservice.model.Feature;
import eu.openanalytics.phaedra.calculationservice.model.FeatureStat;
import eu.openanalytics.phaedra.calculationservice.model.Formula;
import eu.openanalytics.phaedra.calculationservice.model.Protocol;
import eu.openanalytics.phaedra.calculationservice.model.Sequence;
import eu.openanalytics.phaedra.calculationservice.service.ModelMapper;
import eu.openanalytics.phaedra.calculationservice.service.featurestat.FeatureStatExecutor;
import eu.openanalytics.phaedra.calculationservice.service.protocol.FeatureExecutorService;
import eu.openanalytics.phaedra.calculationservice.service.protocol.ProtocolExecutorService;
import eu.openanalytics.phaedra.calculationservice.service.protocol.ProtocolInfoCollector;
import eu.openanalytics.phaedra.calculationservice.service.protocol.SequenceExecutorService;
import eu.openanalytics.phaedra.calculationservice.support.InMemoryResultDataServiceClient;
import eu.openanalytics.phaedra.measurementservice.client.MeasurementServiceClient;
import eu.openanalytics.phaedra.measurementservice.client.exception.MeasUnresolvableException;
import eu.openanalytics.phaedra.plateservice.client.PlateServiceClient;
import eu.openanalytics.phaedra.plateservice.client.exception.PlateUnresolvableException;
import eu.openanalytics.phaedra.plateservice.dto.PlateDTO;
import eu.openanalytics.phaedra.plateservice.dto.WellDTO;
import eu.openanalytics.phaedra.plateservice.enumartion.WellStatus;
import eu.openanalytics.phaedra.protocolservice.client.exception.ProtocolUnresolvableException;
import eu.openanalytics.phaedra.resultdataservice.client.ResultDataServiceClient;
import eu.openanalytics.phaedra.resultdataservice.client.exception.ResultDataUnresolvableException;
import eu.openanalytics.phaedra.resultdataservice.dto.ResultSetDTO;
import eu.openanalytics.phaedra.resultdataservice.enumeration.StatusCode;
import eu.openanalytics.phaedra.scriptengine.client.ScriptEngineClient;
import eu.openanalytics.phaedra.scriptengine.client.model.ScriptExecution;
import eu.openanalytics.phaedra.scriptengine.client.model.TargetRuntime;
import eu.openanalytics.phaedra.scriptengine.dto.ResponseStatusCode;
import eu.openanalytics.phaedra.scriptengine.dto.ScriptExecutionOutputDTO;

@MockitoSettings(strictness = Strictness.STRICT_STUBS)
@ExtendWith(MockitoExtension.class)
public class ProtocolExecutorTest {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private <T> T mockUnimplemented(Class<T> clazz) {
        return mock(clazz, invocation -> {
            throw new IllegalStateException(String.format("[%s:%s] must be stubbed with arguments [%s]!", invocation.getMock().getClass().getSimpleName(), invocation.getMethod().getName(), Arrays.toString(invocation.getArguments())));
        });
    }

    private ResultDataServiceClient resultDataServiceClient;
    private MeasurementServiceClient measurementServiceClient;
    private ScriptEngineClient scriptEngineClient;
    private PlateServiceClient plateServiceClient;
    private FeatureStatExecutor featureStatExecutorService;

    private FeatureExecutorService featureExecutorService;
    private SequenceExecutorService sequenceExecutorService;
    private ProtocolExecutorService protocolExecutorService;
    private ProtocolInfoCollector protocolInfoCollector;
    private final ModelMapper modelMapper = new ModelMapper();

    @BeforeEach
    public void before() throws PlateUnresolvableException {
        resultDataServiceClient = new InMemoryResultDataServiceClient();
        measurementServiceClient = mockUnimplemented(MeasurementServiceClient.class);
        protocolInfoCollector = mockUnimplemented(ProtocolInfoCollector.class);
        scriptEngineClient = mockUnimplemented(ScriptEngineClient.class);
        plateServiceClient = mockUnimplemented(PlateServiceClient.class);
        featureStatExecutorService = mockUnimplemented(FeatureStatExecutor.class);
        featureExecutorService = new FeatureExecutorService(scriptEngineClient, measurementServiceClient, resultDataServiceClient);
        sequenceExecutorService = new SequenceExecutorService(resultDataServiceClient, featureExecutorService, modelMapper, featureStatExecutorService);
        protocolExecutorService = new ProtocolExecutorService(resultDataServiceClient, sequenceExecutorService, protocolInfoCollector, plateServiceClient);
        doReturn(PlateDTO.builder().id(1L).rows(1).columns(4).wells(List.of(
                new WellDTO(1L, 10L, 1, 1, "LC", WellStatus.ACCEPTED_DEFAULT, 1L, "", null),
                new WellDTO(1L, 10L, 1, 2, "SAMPLE", WellStatus.ACCEPTED_DEFAULT, 1L, "", null),
                new WellDTO(1L, 10L, 1, 3, "SAMPLE", WellStatus.ACCEPTED_DEFAULT, 1L, "", null),
                new WellDTO(1L, 10L, 1, 4, "HC", WellStatus.ACCEPTED_DEFAULT, 1L, "", null))
        ).build()).when(plateServiceClient).getPlate(anyLong());

        doReturn(PlateDTO.builder().id(1L).rows(1).columns(4).wells(List.of(
                new WellDTO(1L, 10L, 1, 1, "LC", WellStatus.ACCEPTED_DEFAULT, 1L, "", null),
                new WellDTO(1L, 10L, 1, 2, "SAMPLE", WellStatus.ACCEPTED_DEFAULT, 1L, "", null),
                new WellDTO(1L, 10L, 1, 3, "SAMPLE", WellStatus.ACCEPTED_DEFAULT, 1L, "", null),
                new WellDTO(1L, 10L, 1, 4, "HC", WellStatus.ACCEPTED_DEFAULT, 1L, "", null))
        ).build()).when(plateServiceClient).updatePlateCalculationStatus(any(ResultSetDTO.class));
    }

    @Test
    public void singleFeatureTest() throws Exception {
        var formula = "output <- input$abc * 2";
        var input = new ScriptExecution(new TargetRuntime("R", "fast-lane", "v1"), formula,
                "{\"abc\":[1.0,2.0,3.0,5.0,8.0]}",
                "CalculationService"
        );

        stubGetProtocol(new Protocol(1L, "TestProtocol", null, true, true, "lc", "hc",
                new HashMap<>() {{
                    put(0, new Sequence(0, List.of(new Feature(1L, "Feature1", null, null, "AFormat", FeatureType.CALCULATION, 0,
                            new Formula(1L, "abc_duplicator", null, Category.CALCULATION, formula, ScriptLanguage.R, CalculationScope.WELL, "me", LocalDateTime.now(), "me", LocalDateTime.now()),
                            List.of(new CalculationInputValue(1L, 1L, "abc", null, "abc")), Collections.emptyList()))));
                }}));

        stubGetWellData(4L, "abc", new float[]{1.0f, 2.0f, 3.0f, 5.0f, 8.0f});
        stubNewScriptExecution(R_FAST_LANE, input);
        stubExecute(input);
        stubExecuteFeatureStat();
        completeInputSuccessfully(input, "{\"output\": [2.0,4.0,6.0,10.0,16.0]}");

        var resultSet = protocolExecutorService.execute(1, 1, 4).resultSet().get();
        Assertions.assertEquals(StatusCode.SUCCESS, resultSet.getOutcome());
        Assertions.assertEquals(0L, resultSet.getId());
        Assertions.assertEquals(1L, resultSet.getProtocolId());
        Assertions.assertEquals(4L, resultSet.getMeasId());
        Assertions.assertEquals(1L, resultSet.getPlateId());
        Assertions.assertEquals(Collections.emptyList(), resultSet.getErrors());
        Assertions.assertEquals("", resultSet.getErrorsText());

        var result1 = resultDataServiceClient.getResultData(0, 1);
        Assertions.assertArrayEquals(new float[]{2.0f, 4.0f, 6.0f, 10.0f, 16.0f}, result1.getValues());
        Assertions.assertEquals(StatusCode.SUCCESS, result1.getStatusCode());

        verifyNoMoreInteractions(protocolInfoCollector, measurementServiceClient, scriptEngineClient);
    }

    @Test
    public void getWellDataGivesErrorTest() throws Exception {
        var formula = "output <- input$abc * 2";

        stubGetProtocol(new Protocol(1L, "TestProtocol", null, true, true, "lc", "hc",
                new HashMap<>() {{
                    put(0, new Sequence(0, List.of(new Feature(1L, "Feature1", null, null, "AFormat", FeatureType.CALCULATION, 0,
                            new Formula(1L, "abc_duplicator", null, Category.CALCULATION, formula, ScriptLanguage.R, CalculationScope.WELL, "me", LocalDateTime.now(), "me", LocalDateTime.now()),
                            List.of(new CalculationInputValue(1L, 1L, "abc", null, "abc")), Collections.emptyList()))));
                }}));

        doThrow(new MeasUnresolvableException("WellData not found")).when(measurementServiceClient).getWellData(4L, "abc");

        var resultSet = protocolExecutorService.execute(1, 1, 4).resultSet().get();
        Assertions.assertEquals(StatusCode.FAILURE, resultSet.getOutcome());
        Assertions.assertEquals(0L, resultSet.getId());
        Assertions.assertEquals(1L, resultSet.getProtocolId());
        Assertions.assertEquals(4L, resultSet.getMeasId());
        Assertions.assertEquals(1L, resultSet.getPlateId());
        Assertions.assertEquals(1, resultSet.getErrors().size());
        Assertions.assertNotNull(resultSet.getErrorsText());

        var error = resultSet.getErrors().get(0);
        Assertions.assertEquals("executing sequence => executing feature => collecting variables for feature => retrieving measurement", error.getDescription());
        Assertions.assertEquals("MeasUnresolvableException", error.getExceptionClassName());
        Assertions.assertEquals("WellData not found", error.getExceptionMessage());
        Assertions.assertEquals(1, error.getFeatureId());
        Assertions.assertEquals("Feature1", error.getFeatureName());
        Assertions.assertEquals(0, error.getSequenceNumber());
        Assertions.assertEquals(1, error.getFormulaId());
        Assertions.assertEquals("abc_duplicator", error.getFormulaName());
        Assertions.assertEquals("fromMeasurement", error.getCivType());
        Assertions.assertEquals("abc", error.getCivSource());
        Assertions.assertEquals("abc", error.getCivVariableName());
        Assertions.assertNull(error.getExitCode());
        Assertions.assertNull(error.getStatusMessage());

        Assertions.assertThrows(ResultDataUnresolvableException.class, () -> resultDataServiceClient.getResultData(0, 1));

        verifyNoMoreInteractions(protocolInfoCollector, measurementServiceClient, scriptEngineClient);
    }

    @Test
    public void getResultDataGivesError() throws Exception {
        var mockResultDataServiceClient = mockUnimplemented(ResultDataServiceClient.class);
        featureExecutorService = new FeatureExecutorService(scriptEngineClient, measurementServiceClient, mockResultDataServiceClient);
        sequenceExecutorService = new SequenceExecutorService(resultDataServiceClient, featureExecutorService, modelMapper, featureStatExecutorService);
        protocolExecutorService = new ProtocolExecutorService(resultDataServiceClient, sequenceExecutorService, protocolInfoCollector, plateServiceClient);
        var formula1 = "output <- input$abc * 2";
        var input = new ScriptExecution(new TargetRuntime("R", "fast-lane", "v1"), formula1,
                "{\"abc\":[1.0,2.0,3.0,5.0,8.0]}",
                "CalculationService"
        );
        var formula2 = "output <- input$result * 2";

        stubGetProtocol(new Protocol(1L, "TestProtocol", null, true, true, "lc", "hc",
                new HashMap<>() {{
                    put(0, new Sequence(0, List.of(new Feature(1L, "Feature1", null, null, "AFormat", FeatureType.CALCULATION, 0,
                            new Formula(1L, "abc_duplicator", null, Category.CALCULATION, formula1, ScriptLanguage.R, CalculationScope.WELL, "me", LocalDateTime.now(), "me", LocalDateTime.now()),
                            List.of(new CalculationInputValue(1L, 1L, "abc", null, "abc")), Collections.emptyList()))));
                    put(1, new Sequence(1, List.of(new Feature(2L, "Feature2", null, null, "AFormat", FeatureType.CALCULATION, 1,
                            new Formula(2L, "result_duplicator", null, Category.CALCULATION, formula2, ScriptLanguage.R, CalculationScope.WELL, "me", LocalDateTime.now(), "me", LocalDateTime.now()),
                            List.of(new CalculationInputValue(2L, 2L, null, 1L, "result")), Collections.emptyList()))));
                }}));

        stubGetWellData(4L, "abc", new float[]{1.0f, 2.0f, 3.0f, 5.0f, 8.0f});
        stubNewScriptExecution(R_FAST_LANE, input);
        stubExecute(input);
        stubExecuteFeatureStat();
        completeInputSuccessfully(input, "{\"output\": [2.0,4.0,6.0,10.0,16.0]}");

        doThrow(new ResultDataUnresolvableException("ResultData not found")).when(mockResultDataServiceClient).getResultData(0, 1);

        var resultSet = protocolExecutorService.execute(1, 1, 4).resultSet().get();
        Assertions.assertEquals(StatusCode.FAILURE, resultSet.getOutcome());
        Assertions.assertEquals(0L, resultSet.getId());
        Assertions.assertEquals(1L, resultSet.getProtocolId());
        Assertions.assertEquals(4L, resultSet.getMeasId());
        Assertions.assertEquals(1L, resultSet.getPlateId());
        Assertions.assertEquals(1, resultSet.getErrors().size());
        Assertions.assertNotNull(resultSet.getErrorsText());

        var error = resultSet.getErrors().get(0);
        Assertions.assertEquals("executing sequence => executing feature => collecting variables for feature => retrieving measurement", error.getDescription());
        Assertions.assertEquals("ResultDataUnresolvableException", error.getExceptionClassName());
        Assertions.assertEquals("ResultData not found", error.getExceptionMessage());
        Assertions.assertEquals(2, error.getFeatureId());
        Assertions.assertEquals("Feature2", error.getFeatureName());
        Assertions.assertEquals(1, error.getSequenceNumber());
        Assertions.assertEquals(2, error.getFormulaId());
        Assertions.assertEquals("result_duplicator", error.getFormulaName());
        Assertions.assertEquals("fromFeature", error.getCivType());
        Assertions.assertEquals("1", error.getCivSource());
        Assertions.assertEquals("result", error.getCivVariableName());
        Assertions.assertNull(error.getExitCode());
        Assertions.assertNull(error.getStatusMessage());

        var result1 = resultDataServiceClient.getResultData(0, 1);
        Assertions.assertArrayEquals(new float[]{2.0f, 4.0f, 6.0f, 10.0f, 16.0f}, result1.getValues());
        Assertions.assertEquals(StatusCode.SUCCESS, result1.getStatusCode());

        Assertions.assertThrows(ResultDataUnresolvableException.class, () -> resultDataServiceClient.getResultData(0, 2));

        verifyNoMoreInteractions(protocolInfoCollector, measurementServiceClient, scriptEngineClient);
    }

    @Test
    public void duplicateVariableNameTest() throws Exception {
        var formula1 = "output <- input$abc * 2";

        stubGetProtocol(new Protocol(1L, "TestProtocol", null, true, true, "lc", "hc",
                new HashMap<>() {{
                    put(0, new Sequence(0, List.of(new Feature(1L, "Feature1", null, null, "AFormat", FeatureType.CALCULATION, 0,
                            new Formula(1L, "abc_duplicator", null, Category.CALCULATION, formula1, ScriptLanguage.R, CalculationScope.WELL, "me", LocalDateTime.now(), "me", LocalDateTime.now()),
                            List.of(new CalculationInputValue(1L, 1L, "abc", null, "abc"),
                                    new CalculationInputValue(1L, 1L, "xyz", null, "abc")), Collections.emptyList()))));
                }}));

        stubGetWellData(4L, "abc", new float[]{1.0f, 2.0f, 3.0f, 5.0f, 8.0f});

        var resultSet = protocolExecutorService.execute(1, 1, 4).resultSet().get();
        Assertions.assertEquals(StatusCode.FAILURE, resultSet.getOutcome());
        Assertions.assertEquals(0L, resultSet.getId());
        Assertions.assertEquals(1L, resultSet.getProtocolId());
        Assertions.assertEquals(4L, resultSet.getMeasId());
        Assertions.assertEquals(1L, resultSet.getPlateId());
        Assertions.assertEquals(1, resultSet.getErrors().size());
        Assertions.assertNotNull(resultSet.getErrorsText());

        var error = resultSet.getErrors().get(0);
        Assertions.assertEquals("executing sequence => executing feature => collecting variables for feature => duplicate variable name detected", error.getDescription());
        Assertions.assertNull(error.getExceptionClassName());
        Assertions.assertNull(error.getExceptionMessage());
        Assertions.assertEquals(1, error.getFeatureId());
        Assertions.assertEquals("Feature1", error.getFeatureName());
        Assertions.assertEquals(0, error.getSequenceNumber());
        Assertions.assertEquals(1, error.getFormulaId());
        Assertions.assertEquals("abc_duplicator", error.getFormulaName());
        Assertions.assertEquals("fromMeasurement", error.getCivType());
        Assertions.assertEquals("xyz", error.getCivSource());
        Assertions.assertEquals("abc", error.getCivVariableName());
        Assertions.assertNull(error.getExitCode());
        Assertions.assertNull(error.getStatusMessage());

        Assertions.assertThrows(ResultDataUnresolvableException.class, () -> resultDataServiceClient.getResultData(0, 1));

        verifyNoMoreInteractions(protocolInfoCollector, measurementServiceClient, scriptEngineClient);
    }

    @Test
    public void variableMissingSourceTest() throws Exception {
        var formula1 = "output <- input$abc * 2";

        stubGetProtocol(new Protocol(1L, "TestProtocol", null, true, true, "lc", "hc",
                new HashMap<>() {{
                    put(0, new Sequence(0, List.of(new Feature(1L, "Feature1", null, null, "AFormat", FeatureType.CALCULATION, 0,
                            new Formula(1L, "abc_duplicator", null, Category.CALCULATION, formula1, ScriptLanguage.R, CalculationScope.WELL, "me", LocalDateTime.now(), "me", LocalDateTime.now()),
                            List.of(new CalculationInputValue(1L, 1L, null, null, "abc")), Collections.emptyList()))));
                }}));

        var resultSet = protocolExecutorService.execute(1, 1, 4).resultSet().get();
        Assertions.assertEquals(StatusCode.FAILURE, resultSet.getOutcome());
        Assertions.assertEquals(0L, resultSet.getId());
        Assertions.assertEquals(1L, resultSet.getProtocolId());
        Assertions.assertEquals(4L, resultSet.getMeasId());
        Assertions.assertEquals(1L, resultSet.getPlateId());
        Assertions.assertEquals(1, resultSet.getErrors().size());
        Assertions.assertNotNull(resultSet.getErrorsText());

        var error = resultSet.getErrors().get(0);
        Assertions.assertEquals("executing sequence => executing feature => collecting variables for feature => retrieving measurement => civ has no valid source", error.getDescription());
        Assertions.assertNull(error.getExceptionClassName());
        Assertions.assertNull(error.getExceptionMessage());
        Assertions.assertEquals(1, error.getFeatureId());
        Assertions.assertEquals("Feature1", error.getFeatureName());
        Assertions.assertEquals(0, error.getSequenceNumber());
        Assertions.assertEquals(1, error.getFormulaId());
        Assertions.assertEquals("abc_duplicator", error.getFormulaName());
        Assertions.assertNull(error.getCivType());
        Assertions.assertNull(error.getCivSource());
        Assertions.assertEquals("abc", error.getCivVariableName());
        Assertions.assertNull(error.getExitCode());
        Assertions.assertNull(error.getStatusMessage());

        Assertions.assertThrows(ResultDataUnresolvableException.class, () -> resultDataServiceClient.getResultData(0, 1));

        verifyNoMoreInteractions(protocolInfoCollector, measurementServiceClient, scriptEngineClient);
    }

    @Test
    public void variableUsingFeatureInFirstSequenceTest() throws Exception {
        var formula1 = "output <- input$abc * 2";

        stubGetProtocol(new Protocol(1L, "TestProtocol", null, true, true, "lc", "hc",
                new HashMap<>() {{
                    put(0, new Sequence(0, List.of(new Feature(1L, "Feature1", null, null, "AFormat", FeatureType.CALCULATION, 0,
                            new Formula(1L, "abc_duplicator", null, Category.CALCULATION, formula1, ScriptLanguage.R, CalculationScope.WELL, "me", LocalDateTime.now(), "me", LocalDateTime.now()),
                            List.of(new CalculationInputValue(1L, 1L, null, 1L, "abc")), Collections.emptyList()))));
                }}));

        var resultSet = protocolExecutorService.execute(1, 1, 4).resultSet().get();
        Assertions.assertEquals(StatusCode.FAILURE, resultSet.getOutcome());
        Assertions.assertEquals(0L, resultSet.getId());
        Assertions.assertEquals(1L, resultSet.getProtocolId());
        Assertions.assertEquals(4L, resultSet.getMeasId());
        Assertions.assertEquals(1L, resultSet.getPlateId());
        Assertions.assertEquals(1, resultSet.getErrors().size());
        Assertions.assertNotNull(resultSet.getErrorsText());

        var error = resultSet.getErrors().get(0);
        Assertions.assertEquals("executing sequence => executing feature => collecting variables for feature => retrieving measurement => trying to get feature in sequence 0", error.getDescription());
        Assertions.assertNull(error.getExceptionClassName());
        Assertions.assertNull(error.getExceptionMessage());
        Assertions.assertEquals(1, error.getFeatureId());
        Assertions.assertEquals("Feature1", error.getFeatureName());
        Assertions.assertEquals(0, error.getSequenceNumber());
        Assertions.assertEquals(1, error.getFormulaId());
        Assertions.assertEquals("abc_duplicator", error.getFormulaName());
        Assertions.assertEquals("fromFeature", error.getCivType());
        Assertions.assertEquals("1", error.getCivSource());
        Assertions.assertEquals("abc", error.getCivVariableName());
        Assertions.assertNull(error.getExitCode());
        Assertions.assertNull(error.getStatusMessage());

        Assertions.assertThrows(ResultDataUnresolvableException.class, () -> resultDataServiceClient.getResultData(0, 1));

        verifyNoMoreInteractions(protocolInfoCollector, measurementServiceClient, scriptEngineClient);
    }

    @Test
    public void invalidNumberOfSequencesTest() throws Exception {
        stubGetProtocol(new Protocol(1L, "TestProtocol", null, true, true, "lc", "hc",
                new HashMap<>() {{
                    put(1, new Sequence(1, List.of(new Feature(1L, "Feature1", null, null, "AFormat", FeatureType.CALCULATION, 1,
                            new Formula(1L, "abc_duplicator", null, Category.CALCULATION, "bogus", ScriptLanguage.R, CalculationScope.WELL, "me", LocalDateTime.now(), "me", LocalDateTime.now()),
                            List.of(new CalculationInputValue(1L, 1L, "abc", null, "abc")), Collections.emptyList()))));
                }}));

        var resultSet = protocolExecutorService.execute(1, 1, 4).resultSet().get();
        Assertions.assertEquals(StatusCode.FAILURE, resultSet.getOutcome());
        Assertions.assertEquals(0L, resultSet.getId());
        Assertions.assertEquals(1L, resultSet.getProtocolId());
        Assertions.assertEquals(4L, resultSet.getMeasId());
        Assertions.assertEquals(1L, resultSet.getPlateId());
        Assertions.assertEquals(1, resultSet.getErrors().size());
        Assertions.assertNotNull(resultSet.getErrorsText());

        var error = resultSet.getErrors().get(0);
        Assertions.assertEquals("executing protocol => missing sequence", error.getDescription());
        Assertions.assertNull(error.getExceptionClassName());
        Assertions.assertNull(error.getExceptionMessage());
        Assertions.assertNull(error.getFeatureId());
        Assertions.assertNull(error.getFeatureName());
        Assertions.assertEquals(0, error.getSequenceNumber());
        Assertions.assertNull(error.getFormulaId());
        Assertions.assertNull(error.getFormulaName());
        Assertions.assertNull(error.getCivType());
        Assertions.assertNull(error.getCivSource());
        Assertions.assertNull(error.getCivVariableName());
        Assertions.assertNull(error.getExitCode());
        Assertions.assertNull(error.getStatusMessage());

        Assertions.assertThrows(ResultDataUnresolvableException.class, () -> resultDataServiceClient.getResultData(0, 1));

        verifyNoMoreInteractions(protocolInfoCollector, measurementServiceClient, scriptEngineClient);
    }

    @Test
    public void scriptErrorTest() throws Exception {
        var formula = "output <- input$abc * "; // invalid script
        var input = new ScriptExecution(new TargetRuntime("R", "fast-lane", "v1"), formula,
                "{\"abc\":[1.0,2.0,3.0,5.0,8.0]}",
                "CalculationService"
        );

        stubGetProtocol(new Protocol(1L, "TestProtocol", null, true, true, "lc", "hc",
                new HashMap<>() {{
                    put(0, new Sequence(0, List.of(new Feature(1L, "Feature1", null, null, "AFormat", FeatureType.CALCULATION, 0,
                            new Formula(1L, "abc_duplicator", null, Category.CALCULATION, formula, ScriptLanguage.R, CalculationScope.WELL, "me", LocalDateTime.now(), "me", LocalDateTime.now()),
                            List.of(new CalculationInputValue(1L, 1L, "abc", null, "abc")), Collections.emptyList()))));
                }}));

        stubGetWellData(4L, "abc", new float[]{1.0f, 2.0f, 3.0f, 5.0f, 8.0f});
        stubNewScriptExecution(R_FAST_LANE, input);
        stubExecute(input);
        completeInputScriptError(input);

        var resultSet = protocolExecutorService.execute(1, 1, 4).resultSet().get();
        Assertions.assertEquals(StatusCode.FAILURE, resultSet.getOutcome());
        Assertions.assertEquals(0L, resultSet.getId());
        Assertions.assertEquals(1L, resultSet.getProtocolId());
        Assertions.assertEquals(4L, resultSet.getMeasId());
        Assertions.assertEquals(1L, resultSet.getPlateId());
        Assertions.assertEquals(1, resultSet.getErrors().size());
        Assertions.assertNotNull(resultSet.getErrorsText());

        var error = resultSet.getErrors().get(0);
        Assertions.assertEquals("executing sequence => processing output => output indicates error [SCRIPT_ERROR]", error.getDescription());
        Assertions.assertNull(error.getExceptionClassName());
        Assertions.assertNull(error.getExceptionMessage());
        Assertions.assertEquals(1, error.getFeatureId());
        Assertions.assertEquals("Feature1", error.getFeatureName());
        Assertions.assertEquals(0, error.getSequenceNumber());
        Assertions.assertEquals(1, error.getFormulaId());
        Assertions.assertEquals("abc_duplicator", error.getFormulaName());
        Assertions.assertNull(error.getCivType());
        Assertions.assertNull(error.getCivSource());
        Assertions.assertNull(error.getCivVariableName());
        Assertions.assertEquals(42, error.getExitCode());
        Assertions.assertEquals("Script did not create output file!", error.getStatusMessage());

        var result1 = resultDataServiceClient.getResultData(0, 1);
        Assertions.assertArrayEquals(new float[]{}, result1.getValues());
        Assertions.assertEquals(StatusCode.FAILURE, result1.getStatusCode());

        verifyNoMoreInteractions(protocolInfoCollector, measurementServiceClient, scriptEngineClient);
    }

    @Test
    public void scriptReturnsInvalidOutputTest() throws Exception {
        var formula = "output <- input$abc * 2";
        var input = new ScriptExecution(new TargetRuntime("R", "fast-lane", "v1"), formula,
                "{\"abc\":[1.0,2.0,3.0,5.0,8.0]}",
                "CalculationService"
        );

        stubGetProtocol(new Protocol(1L, "TestProtocol", null, true, true, "lc", "hc",
                new HashMap<>() {{
                    put(0, new Sequence(0, List.of(new Feature(1L, "Feature1", null, null, "AFormat", FeatureType.CALCULATION, 0,
                            new Formula(1L, "abc_duplicator", null, Category.CALCULATION, formula, ScriptLanguage.R, CalculationScope.WELL, "me", LocalDateTime.now(), "me", LocalDateTime.now()),
                            List.of(new CalculationInputValue(1L, 1L, "abc", null, "abc")), Collections.emptyList()))));
                }}));

        stubGetWellData(4L, "abc", new float[]{1.0f, 2.0f, 3.0f, 5.0f, 8.0f});
        stubNewScriptExecution(R_FAST_LANE, input);
        stubExecute(input);
        completeInputSuccessfully(input, "{\"output\": [2.0,4.0,6.0,10.0,16.0}"); // invalid output!

        var resultSet = protocolExecutorService.execute(1, 1, 4).resultSet().get();
        Assertions.assertEquals(StatusCode.FAILURE, resultSet.getOutcome());
        Assertions.assertEquals(0L, resultSet.getId());
        Assertions.assertEquals(1L, resultSet.getProtocolId());
        Assertions.assertEquals(4L, resultSet.getMeasId());
        Assertions.assertEquals(1L, resultSet.getPlateId());
        Assertions.assertEquals(1, resultSet.getErrors().size());
        Assertions.assertNotNull(resultSet.getErrorsText());

        var error = resultSet.getErrors().get(0);
        Assertions.assertEquals("executing sequence => processing output => parsing output", error.getDescription());
        Assertions.assertEquals("JsonMappingException", error.getExceptionClassName());
        Assertions.assertNotNull(error.getExceptionMessage());
        Assertions.assertEquals(1, error.getFeatureId());
        Assertions.assertEquals("Feature1", error.getFeatureName());
        Assertions.assertEquals(0, error.getSequenceNumber());
        Assertions.assertEquals(1, error.getFormulaId());
        Assertions.assertEquals("abc_duplicator", error.getFormulaName());
        Assertions.assertNull(error.getCivType());
        Assertions.assertNull(error.getCivSource());
        Assertions.assertNull(error.getCivVariableName());
        Assertions.assertEquals(0, error.getExitCode());
        Assertions.assertEquals("Ok", error.getStatusMessage());

        Assertions.assertThrows(ResultDataUnresolvableException.class, () -> resultDataServiceClient.getResultData(0, 1));

        verifyNoMoreInteractions(protocolInfoCollector, measurementServiceClient, scriptEngineClient);
    }

    @Test
    public void exceptionDuringExecution() throws Exception {
        var formula = "output <- input$abc * 2";
        var input = new ScriptExecution(new TargetRuntime("R", "fast-lane", "v1"), formula,
                "{\"abc\":[1.0,2.0,3.0,5.0,8.0]}",
                "CalculationService"
        );

        stubGetProtocol(new Protocol(1L, "TestProtocol", null, true, true, "lc", "hc",
                new HashMap<>() {{
                    put(0, new Sequence(0, List.of(new Feature(1L, "Feature1", null, null, "AFormat", FeatureType.CALCULATION, 0,
                            new Formula(1L, "abc_duplicator", null, Category.CALCULATION, formula, ScriptLanguage.R, CalculationScope.WELL, "me", LocalDateTime.now(), "me", LocalDateTime.now()),
                            List.of(new CalculationInputValue(1L, 1L, "abc", null, "abc")), Collections.emptyList()))));
                }}));

        stubGetWellData(4L, "abc", new float[]{1.0f, 2.0f, 3.0f, 5.0f, 8.0f});
        stubNewScriptExecution(R_FAST_LANE, input);
        stubExecute(input);
        input.getOutput().completeExceptionally(new RuntimeException("Some error during execution!"));

        var resultSet = protocolExecutorService.execute(1, 1, 4).resultSet().get();
        Assertions.assertEquals(StatusCode.FAILURE, resultSet.getOutcome());
        Assertions.assertEquals(0L, resultSet.getId());
        Assertions.assertEquals(1L, resultSet.getProtocolId());
        Assertions.assertEquals(4L, resultSet.getMeasId());
        Assertions.assertEquals(1L, resultSet.getPlateId());
        Assertions.assertEquals(1, resultSet.getErrors().size());
        Assertions.assertNotNull(resultSet.getErrorsText());

        var error = resultSet.getErrors().get(0);
        Assertions.assertEquals("executing sequence => waiting for output to be received => exception during execution", error.getDescription());
        Assertions.assertEquals("RuntimeException", error.getExceptionClassName());
        Assertions.assertEquals("Some error during execution!", error.getExceptionMessage());
        Assertions.assertEquals(1, error.getFeatureId());
        Assertions.assertEquals("Feature1", error.getFeatureName());
        Assertions.assertEquals(0, error.getSequenceNumber());
        Assertions.assertEquals(1, error.getFormulaId());
        Assertions.assertEquals("abc_duplicator", error.getFormulaName());
        Assertions.assertNull(error.getCivType());
        Assertions.assertNull(error.getCivSource());
        Assertions.assertNull(error.getCivVariableName());
        Assertions.assertNull(error.getExitCode());

        Assertions.assertThrows(ResultDataUnresolvableException.class, () -> resultDataServiceClient.getResultData(0, 1));

        verifyNoMoreInteractions(protocolInfoCollector, measurementServiceClient, scriptEngineClient);
    }

    @Test
    public void exceptionDuringExecutionMultipleFeatures() throws Exception {
        var formula1 = "output <- input$abc * 2";
        var formula2 = "output <- input$abc * 3";
        var formula3 = "output <- input$abc * 4";
        var input1 = new ScriptExecution(new TargetRuntime("R", "fast-lane", "v1"), formula1,
                "{\"abc\":[1.0,2.0,3.0,5.0,8.0]}",
                "CalculationService"
        );
        var input2 = new ScriptExecution(new TargetRuntime("R", "fast-lane", "v1"), formula2,
                "{\"abc\":[1.0,2.0,3.0,5.0,8.0]}",
                "CalculationService"
        );
        var input3 = new ScriptExecution(new TargetRuntime("R", "fast-lane", "v1"), formula3,
                "{\"abc\":[1.0,2.0,3.0,5.0,8.0]}",
                "CalculationService"
        );

        stubGetProtocol(new Protocol(1L, "TestProtocol", null, true, true, "lc", "hc",
                new HashMap<>() {{
                    put(0, new Sequence(0, List.of(
                            new Feature(1L, "Feature1", null, null, "AFormat", FeatureType.CALCULATION, 0,
                                    new Formula(1L, "abc_duplicator", null, Category.CALCULATION, formula1, ScriptLanguage.R, CalculationScope.WELL, "me", LocalDateTime.now(), "me", LocalDateTime.now()),
                                    List.of(new CalculationInputValue(1L, 1L, "abc", null, "abc")), Collections.emptyList()),
                            new Feature(2L, "Feature2", null, null, "AFormat", FeatureType.CALCULATION, 0,
                                    new Formula(2L, "abc_times_three", null, Category.CALCULATION, formula2, ScriptLanguage.R, CalculationScope.WELL, "me", LocalDateTime.now(), "me", LocalDateTime.now()),
                                    List.of(new CalculationInputValue(2L, 2L, "abc", null, "abc")), Collections.emptyList()),
                            new Feature(3L, "Feature2", null, null, "AFormat", FeatureType.CALCULATION, 0,
                                    new Formula(3L, "abc_times_four", null, Category.CALCULATION, formula3, ScriptLanguage.R, CalculationScope.WELL, "me", LocalDateTime.now(), "me", LocalDateTime.now()),
                                    List.of(new CalculationInputValue(3L, 3L, "abc", null, "abc")), Collections.emptyList()))
                    ));
                }}));

        stubGetWellData(4L, "abc", new float[]{1.0f, 2.0f, 3.0f, 5.0f, 8.0f});

        stubNewScriptExecution(R_FAST_LANE, input1);
        stubNewScriptExecution(R_FAST_LANE, input2);
        stubNewScriptExecution(R_FAST_LANE, input3);
        stubExecute(input1);
        stubExecute(input2);
        stubExecute(input3);
        stubExecuteFeatureStat();

        // input 1: return value after 100 ms
        completeInputSuccessfullyWithDelay(input1, "{\"output\": [2.0,4.0,6.0,10.0,16.0]}", 100);

        // input 2: return error after 250 ms
        completeInputWithExceptionAndDelay(input2, new RuntimeException("Some error during execution!"), 250);

        // input 3: return value after 1000ms -> should NOT get cancelled
        completeInputSuccessfullyWithDelay(input3, "{\"output\": [4.0,8.0,12.0,20.0,32.0]}", 1000);

        var resultSet = protocolExecutorService.execute(1, 1, 4).resultSet().get();

        Assertions.assertFalse(input1.getOutput().isCancelled());
        Assertions.assertFalse(input2.getOutput().isCancelled());
        Assertions.assertFalse(input3.getOutput().isCancelled());

        Assertions.assertEquals(StatusCode.FAILURE, resultSet.getOutcome());
        Assertions.assertEquals(0L, resultSet.getId());
        Assertions.assertEquals(1L, resultSet.getProtocolId());
        Assertions.assertEquals(4L, resultSet.getMeasId());
        Assertions.assertEquals(1L, resultSet.getPlateId());
        Assertions.assertEquals(1, resultSet.getErrors().size());
        Assertions.assertNotNull(resultSet.getErrorsText());

        var error = resultSet.getErrors().get(0);
        Assertions.assertEquals("executing sequence => waiting for output to be received => exception during execution", error.getDescription());
        Assertions.assertEquals("RuntimeException", error.getExceptionClassName());
        Assertions.assertEquals("Some error during execution!", error.getExceptionMessage());
        Assertions.assertEquals(2, error.getFeatureId());
        Assertions.assertEquals("Feature2", error.getFeatureName());
        Assertions.assertEquals(0, error.getSequenceNumber());
        Assertions.assertEquals(2, error.getFormulaId());
        Assertions.assertEquals("abc_times_three", error.getFormulaName());
        Assertions.assertNull(error.getCivType());
        Assertions.assertNull(error.getCivSource());
        Assertions.assertNull(error.getCivVariableName());
        Assertions.assertNull(error.getExitCode());

        // check resultData
        var result1 = resultDataServiceClient.getResultData(0, 1);
        Assertions.assertArrayEquals(new float[]{2.0f, 4.0f, 6.0f, 10.0f, 16.0f}, result1.getValues());
        Assertions.assertEquals(StatusCode.SUCCESS, result1.getStatusCode());

        Assertions.assertThrows(ResultDataUnresolvableException.class, () -> resultDataServiceClient.getResultData(0, 2));

        var result3 = resultDataServiceClient.getResultData(0, 3);
        Assertions.assertArrayEquals(new float[]{4.0f, 8.0f, 12.0f, 20.0f, 32.0f}, result3.getValues());
        Assertions.assertEquals(StatusCode.SUCCESS, result3.getStatusCode());

        verifyNoMoreInteractions(protocolInfoCollector, measurementServiceClient, scriptEngineClient);
    }

    @Test
    public void errorDuringSendingShouldCancelOutputs() throws Exception {
        var formula1 = "output <- input$abc * 2";
        var formula2 = "output <- input$abc * 3";
        var formula3 = "output <- input$abc * 4";
        var input1 = new ScriptExecution(new TargetRuntime("R", "fast-lane", "v1"), formula1,
                "{\"abc\":[1.0,2.0,3.0,5.0,8.0]}",
                "CalculationService"
        );
        var input2 = new ScriptExecution(new TargetRuntime("R", "fast-lane", "v1"), formula2,
                "{\"abc\":[1.0,2.0,3.0,5.0,8.0]}",
                "CalculationService"
        );
        var input3 = new ScriptExecution(new TargetRuntime("R", "fast-lane", "v1"), formula3,
                "{\"abc\":[1.0,2.0,3.0,5.0,8.0]}",
                "CalculationService"
        );

        stubGetProtocol(new Protocol(1L, "TestProtocol", null, true, true, "lc", "hc",
                new HashMap<>() {{
                    put(0, new Sequence(0, List.of(
                            new Feature(1L, "Feature1", null, null, "AFormat", FeatureType.CALCULATION, 0,
                                    new Formula(1L, "abc_duplicator", null, Category.CALCULATION, formula1, ScriptLanguage.R, CalculationScope.WELL, "me", LocalDateTime.now(), "me", LocalDateTime.now()),
                                    List.of(new CalculationInputValue(1L, 1L, "abc", null, "abc")), Collections.emptyList()),
                            new Feature(2L, "Feature2", null, null, "AFormat", FeatureType.CALCULATION, 0,
                                    new Formula(2L, "abc_times_three", null, Category.CALCULATION, formula2, ScriptLanguage.R, CalculationScope.WELL, "me", LocalDateTime.now(), "me", LocalDateTime.now()),
                                    List.of(new CalculationInputValue(2L, 2L, "abc", null, "abc")), Collections.emptyList()),
                            new Feature(3L, "Feature2", null, null, "AFormat", FeatureType.CALCULATION, 0,
                                    new Formula(3L, "abc_times_four", null, Category.CALCULATION, formula3, ScriptLanguage.R, CalculationScope.WELL, "me", LocalDateTime.now(), "me", LocalDateTime.now()),
                                    List.of(new CalculationInputValue(3L, 3L, "abc", null, "abc")), Collections.emptyList()))
                    ));
                }}));

        stubGetWellData(4L, "abc", new float[]{1.0f, 2.0f, 3.0f, 5.0f, 8.0f});

        stubNewScriptExecution(R_FAST_LANE, input1);
        stubNewScriptExecution(R_FAST_LANE, input2);
        stubNewScriptExecution(R_FAST_LANE, input3);
        stubExecute(input1);
        stubExecuteWithExceptionAndDelay(input2, new RuntimeException("Error during sending of execution!"), 1000);
        stubExecute(input3);
        stubExecuteFeatureStat();

        // input 1: return value after 100 ms
        completeInputSuccessfullyWithDelay(input1, "{\"output\": [2.0,4.0,6.0,10.0,16.0]}", 100);

        // input 3:  return value after 1000ms -> should NOT get cancelled
        completeInputSuccessfullyWithDelay(input3, "{\"output\": [4.0,8.0,12.0,20.0,32.0]}", 1000);

        var resultSet = protocolExecutorService.execute(1, 1, 4).resultSet().get();

        Assertions.assertFalse(input1.getOutput().isCancelled()); //
        Assertions.assertFalse(input2.getOutput().isCancelled());
        Assertions.assertFalse(input2.getOutput().isDone());
        Assertions.assertFalse(input2.getOutput().isCompletedExceptionally());
        Assertions.assertFalse(input3.getOutput().isCancelled());

        Assertions.assertEquals(StatusCode.FAILURE, resultSet.getOutcome());
        Assertions.assertEquals(0L, resultSet.getId());
        Assertions.assertEquals(1L, resultSet.getProtocolId());
        Assertions.assertEquals(4L, resultSet.getMeasId());
        Assertions.assertEquals(1L, resultSet.getPlateId());
        Assertions.assertEquals(1, resultSet.getErrors().size());
        Assertions.assertNotNull(resultSet.getErrorsText());

        var error = resultSet.getErrors().get(0);
        Assertions.assertEquals("executing sequence => waiting for feature to be sent => exception during execution", error.getDescription());
        Assertions.assertEquals("RuntimeException", error.getExceptionClassName());
        Assertions.assertEquals("Error during sending of execution!", error.getExceptionMessage());
        Assertions.assertEquals(2, error.getFeatureId());
        Assertions.assertEquals("Feature2", error.getFeatureName());
        Assertions.assertEquals(0, error.getSequenceNumber());
        Assertions.assertEquals(2, error.getFormulaId());
        Assertions.assertEquals("abc_times_three", error.getFormulaName());
        Assertions.assertNull(error.getCivType());
        Assertions.assertNull(error.getCivSource());
        Assertions.assertNull(error.getCivVariableName());
        Assertions.assertNull(error.getExitCode());

        // check resultData
        var result1 = resultDataServiceClient.getResultData(0, 1);
        Assertions.assertArrayEquals(new float[]{2.0f, 4.0f, 6.0f, 10.0f, 16.0f}, result1.getValues());
        Assertions.assertEquals(StatusCode.SUCCESS, result1.getStatusCode());

        Assertions.assertThrows(ResultDataUnresolvableException.class, () -> resultDataServiceClient.getResultData(0, 2));

        var result3 = resultDataServiceClient.getResultData(0, 3);
        Assertions.assertArrayEquals(new float[]{4.0f, 8.0f, 12.0f, 20.0f, 32.0f}, result3.getValues());
        Assertions.assertEquals(StatusCode.SUCCESS, result3.getStatusCode());

        verifyNoMoreInteractions(protocolInfoCollector, measurementServiceClient, scriptEngineClient);
    }

    @Test
    public void threadInterruptedWhileWaitingSubmittingScriptTest() throws Exception {
        var formula = "output <- input$abc * 2";
        var input = new ScriptExecution(new TargetRuntime("R", "fast-lane", "v1"), formula,
                "{\"abc\":[1.0,2.0,3.0,5.0,8.0]}",
                "CalculationService"
        );

        stubGetProtocol(new Protocol(1L, "TestProtocol", null, true, true, "lc", "hc",
                new HashMap<>() {{
                    put(0, new Sequence(0, List.of(new Feature(1L, "Feature1", null, null, "AFormat", FeatureType.CALCULATION, 0,
                            new Formula(1L, "abc_duplicator", null, Category.CALCULATION, formula, ScriptLanguage.R, CalculationScope.WELL, "me", LocalDateTime.now(), "me", LocalDateTime.now()),
                            List.of(new CalculationInputValue(1L, 1L, "abc", null, "abc")), Collections.emptyList()))));
                }}));

        stubGetWellData(4L, "abc", new float[]{1.0f, 2.0f, 3.0f, 5.0f, 8.0f});
        stubNewScriptExecution(R_FAST_LANE, input);

        doAnswer((it) -> {
            // cancel pool when waiting for input to be sent
            logger.info("Sleeping instead of sending input");
            Thread.sleep(10_000);
            return true;
        }).when(scriptEngineClient).execute(input);

        // kill main thread when waiting for featureStat to finish
        var mainThread = Thread.currentThread();
        new Timer().schedule(new TimerTask() {
            @Override
            public void run() {
                logger.info("Interrupting mainthread...");
                mainThread.interrupt();
            }
        }, 5000);

        var resultSetIdFuture = new CompletableFuture<Long>();
        var resultSet = protocolExecutorService.executeProtocol(resultSetIdFuture, 1, 1, 4);
        Assertions.assertEquals(StatusCode.FAILURE, resultSet.getOutcome());
        Assertions.assertEquals(0L, resultSet.getId());
        Assertions.assertEquals(1L, resultSet.getProtocolId());
        Assertions.assertEquals(4L, resultSet.getMeasId());
        Assertions.assertEquals(1L, resultSet.getPlateId());
        Assertions.assertEquals(1, resultSet.getErrors().size());
        Assertions.assertNotNull(resultSet.getErrorsText());

        var error = resultSet.getErrors().get(0);
        Assertions.assertEquals("executing sequence => waiting for feature to be sent => interrupted", error.getDescription());
        Assertions.assertEquals("InterruptedException", error.getExceptionClassName());
        Assertions.assertNull(error.getExceptionMessage());
        Assertions.assertEquals(1, error.getFeatureId());
        Assertions.assertEquals("Feature1", error.getFeatureName());
        Assertions.assertEquals(0, error.getSequenceNumber());
        Assertions.assertEquals(1, error.getFormulaId());
        Assertions.assertEquals("abc_duplicator", error.getFormulaName());
        Assertions.assertNull(error.getCivType());
        Assertions.assertNull(error.getCivSource());
        Assertions.assertNull(error.getCivVariableName());
        Assertions.assertNull(error.getExitCode());

        Assertions.assertThrows(ResultDataUnresolvableException.class, () -> resultDataServiceClient.getResultData(0, 1));

        verifyNoMoreInteractions(protocolInfoCollector, measurementServiceClient, scriptEngineClient);
    }

    @Test
    public void threadInterruptedWhileWaitingForOutputTest() throws Exception {
        var formula = "output <- input$abc * 2";
        var input = new ScriptExecution(new TargetRuntime("R", "fast-lane", "v1"), formula,
                "{\"abc\":[1.0,2.0,3.0,5.0,8.0]}",
                "CalculationService"
        );

        stubGetProtocol(new Protocol(1L, "TestProtocol", null, true, true, "lc", "hc",
                new HashMap<>() {{
                    put(0, new Sequence(0, List.of(new Feature(1L, "Feature1", null, null, "AFormat", FeatureType.CALCULATION, 0,
                            new Formula(1L, "abc_duplicator", null, Category.CALCULATION, formula, ScriptLanguage.R, CalculationScope.WELL, "me", LocalDateTime.now(), "me", LocalDateTime.now()),
                            List.of(new CalculationInputValue(1L, 1L, "abc", null, "abc")), Collections.emptyList()))));
                }}));

        stubGetWellData(4L, "abc", new float[]{1.0f, 2.0f, 3.0f, 5.0f, 8.0f});
        stubNewScriptExecution(R_FAST_LANE, input);
        stubExecute(input);

        new Timer().schedule(new TimerTask() {
            @Override
            public void run() {
                // cancel pool when waiting for output
                protocolExecutorService.getExecutorService().shutdownNow();
            }
        }, 2000);

        var resultSet = protocolExecutorService.execute(1, 1, 4).resultSet().get();
        Assertions.assertEquals(StatusCode.FAILURE, resultSet.getOutcome());
        Assertions.assertEquals(0L, resultSet.getId());
        Assertions.assertEquals(1L, resultSet.getProtocolId());
        Assertions.assertEquals(4L, resultSet.getMeasId());
        Assertions.assertEquals(1L, resultSet.getPlateId());
        Assertions.assertEquals(1, resultSet.getErrors().size());
        Assertions.assertNotNull(resultSet.getErrorsText());

        var error = resultSet.getErrors().get(0);
        Assertions.assertEquals("executing sequence => waiting for output to be received => interrupted", error.getDescription());
        Assertions.assertEquals("InterruptedException", error.getExceptionClassName());
        Assertions.assertNull(error.getExceptionMessage());
        Assertions.assertEquals(1, error.getFeatureId());
        Assertions.assertEquals("Feature1", error.getFeatureName());
        Assertions.assertEquals(0, error.getSequenceNumber());
        Assertions.assertEquals(1, error.getFormulaId());
        Assertions.assertEquals("abc_duplicator", error.getFormulaName());
        Assertions.assertNull(error.getCivType());
        Assertions.assertNull(error.getCivSource());
        Assertions.assertNull(error.getCivVariableName());
        Assertions.assertNull(error.getExitCode());

        Assertions.assertThrows(ResultDataUnresolvableException.class, () -> resultDataServiceClient.getResultData(0, 1));

        verifyNoMoreInteractions(protocolInfoCollector, measurementServiceClient, scriptEngineClient);
    }

    @Test
    public void futureCancelledWhileWaitingForOutputTest() throws Exception {
        var formula = "output <- input$abc * 2";
        var input = new ScriptExecution(new TargetRuntime("R", "fast-lane", "v1"), formula,
                "{\"abc\":[1.0,2.0,3.0,5.0,8.0]}",
                "CalculationService"
        );

        stubGetProtocol(new Protocol(1L, "TestProtocol", null, true, true, "lc", "hc",
                new HashMap<>() {{
                    put(0, new Sequence(0, List.of(new Feature(1L, "Feature1", null, null, "AFormat", FeatureType.CALCULATION, 0,
                            new Formula(1L, "abc_duplicator", null, Category.CALCULATION, formula, ScriptLanguage.R, CalculationScope.WELL, "me", LocalDateTime.now(), "me", LocalDateTime.now()),
                            List.of(new CalculationInputValue(1L, 1L, "abc", null, "abc")), Collections.emptyList()))));
                }}));

        stubGetWellData(4L, "abc", new float[]{1.0f, 2.0f, 3.0f, 5.0f, 8.0f});
        stubNewScriptExecution(R_FAST_LANE, input);
        stubExecute(input);

        new Timer().schedule(new TimerTask() {
            @Override
            public void run() {
                // cancel output future when waiting for output
                input.getOutput().cancel(true);
            }
        }, 2000);

        var resultSet = protocolExecutorService.execute(1, 1, 4).resultSet().get();
        Assertions.assertEquals(StatusCode.FAILURE, resultSet.getOutcome());
        Assertions.assertEquals(0L, resultSet.getId());
        Assertions.assertEquals(1L, resultSet.getProtocolId());
        Assertions.assertEquals(4L, resultSet.getMeasId());
        Assertions.assertEquals(1L, resultSet.getPlateId());
        Assertions.assertEquals(1, resultSet.getErrors().size());
        Assertions.assertNotNull(resultSet.getErrorsText());

        var error = resultSet.getErrors().get(0);
        Assertions.assertEquals("executing sequence => waiting for output to be received => exception during execution", error.getDescription());
        Assertions.assertEquals("CancellationException", error.getExceptionClassName());
        Assertions.assertNull(error.getExceptionMessage());
        Assertions.assertEquals(1, error.getFeatureId());
        Assertions.assertEquals("Feature1", error.getFeatureName());
        Assertions.assertEquals(0, error.getSequenceNumber());
        Assertions.assertEquals(1, error.getFormulaId());
        Assertions.assertEquals("abc_duplicator", error.getFormulaName());
        Assertions.assertNull(error.getCivType());
        Assertions.assertNull(error.getCivSource());
        Assertions.assertNull(error.getCivVariableName());
        Assertions.assertNull(error.getExitCode());

        Assertions.assertThrows(ResultDataUnresolvableException.class, () -> resultDataServiceClient.getResultData(0, 1));

        verifyNoMoreInteractions(protocolInfoCollector, measurementServiceClient, scriptEngineClient);
    }

    @Test
    public void singleFeatureTestWithFeatureStatInterrupted() throws Exception {
        var formula = "output <- input$abc * 2";
        var input = new ScriptExecution(new TargetRuntime("R", "fast-lane", "v1"), formula,
                "{\"abc\":[1.0,2.0,3.0,5.0,8.0]}",
                "CalculationService"
        );

        var featureStatFormula = Formula.builder()
                .formula("JavaStat::count")
                .category(Category.CALCULATION)
                .name("count")
                .id(1L)
                .language(ScriptLanguage.JAVASTAT)
                .scope(CalculationScope.PLATE)
                .build();

        var featureStat = FeatureStat.builder()
                .id(featureStatFormula.getId())
                .formula(featureStatFormula)
                .featureId(1L)
                .plateStat(true)
                .welltypeStat(true)
                .name(featureStatFormula.getName()).build();

        stubGetProtocol(new Protocol(1L, "TestProtocol", null, true, true, "lc", "hc",
                new HashMap<>() {
                    {
                        put(0, new Sequence(0, List.of(new Feature(1L, "Feature1", null, null, "AFormat", FeatureType.CALCULATION, 0,
                                new Formula(1L, "abc_duplicator", null, Category.CALCULATION, formula, ScriptLanguage.R, CalculationScope.WELL, "me", LocalDateTime.now(), "me", LocalDateTime.now()),
                                List.of(new CalculationInputValue(1L, 1L, "abc", null, "abc")), List.of(featureStat)))));
                    }
                }));

        stubGetWellData(4L, "abc", new float[]{1.0f, 2.0f, 3.0f, 5.0f, 8.0f});

        stubNewScriptExecution(R_FAST_LANE, input);

        stubExecute(input);

        // mock the featureStatExecutorService
        doAnswer((it) -> {
            logger.info("Sleeping instead of executing featureStat");
            Thread.sleep(10_000);
            return true;
        }).when(featureStatExecutorService).executeFeatureStat(any(), any(), any());

        // kill main thread when waiting for featureStat to finish
        var mainThread = Thread.currentThread();
        new Timer().schedule(new TimerTask() {
            @Override
            public void run() {
                logger.info("Interrupting mainthread...");
                mainThread.interrupt();
            }
        }, 5000);

        completeInputSuccessfully(input, "{\"output\": [2.0,4.0,6.0,10.0,16.0]}");

        var resultSetIdFuture = new CompletableFuture<Long>();
        var resultSet = protocolExecutorService.executeProtocol(resultSetIdFuture, 1, 1, 4);
        Assertions.assertEquals(StatusCode.FAILURE, resultSet.getOutcome());
        Assertions.assertEquals(0L, resultSet.getId());
        Assertions.assertEquals(1L, resultSet.getProtocolId());
        Assertions.assertEquals(4L, resultSet.getMeasId());
        Assertions.assertEquals(1L, resultSet.getPlateId());
        Assertions.assertEquals(1, resultSet.getErrors().size());
        Assertions.assertNotNull(resultSet.getErrorsText());

        var result1 = resultDataServiceClient.getResultData(0, 1);
        Assertions.assertArrayEquals(new float[]{2.0f, 4.0f, 6.0f, 10.0f, 16.0f}, result1.getValues());
        Assertions.assertEquals(StatusCode.SUCCESS, result1.getStatusCode());

        var error = resultSet.getErrors().get(0);
        Assertions.assertEquals("executing protocol => waiting for calculations of featureStats of a feature to complete => interrupted", error.getDescription());
        Assertions.assertEquals("InterruptedException", error.getExceptionClassName());
        Assertions.assertNull(error.getExceptionMessage());
        Assertions.assertEquals(1, error.getFeatureId());

        verifyNoMoreInteractions(protocolInfoCollector, measurementServiceClient, scriptEngineClient);
    }


    @Test
    public void singleFeatureTestWithFeatureStatCancelled() throws Exception {
        var formula = "output <- input$abc * 2";
        var input = new ScriptExecution(new TargetRuntime("R", "fast-lane", "v1"), formula,
                "{\"abc\":[1.0,2.0,3.0,5.0,8.0]}",
                "CalculationService"
        );

        var featureStatFormula = Formula.builder()
                .formula("JavaStat::count")
                .category(Category.CALCULATION)
                .name("count")
                .id(1L)
                .language(ScriptLanguage.JAVASTAT)
                .scope(CalculationScope.PLATE)
                .build();

        var featureStat = FeatureStat.builder()
                .id(featureStatFormula.getId())
                .formula(featureStatFormula)
                .featureId(1L)
                .plateStat(true)
                .welltypeStat(true)
                .name(featureStatFormula.getName()).build();

        stubGetProtocol(new Protocol(1L, "TestProtocol", null, true, true, "lc", "hc",
                new HashMap<>() {
                    {
                        put(0, new Sequence(0, List.of(new Feature(1L, "Feature1", null, null, "AFormat", FeatureType.CALCULATION, 0,
                                new Formula(1L, "abc_duplicator", null, Category.CALCULATION, formula, ScriptLanguage.R, CalculationScope.WELL, "me", LocalDateTime.now(), "me", LocalDateTime.now()),
                                List.of(new CalculationInputValue(1L, 1L, "abc", null, "abc")), List.of(featureStat)))));
                    }
                }));

        stubGetWellData(4L, "abc", new float[]{
                1.0f, 2.0f, 3.0f, 5.0f, 8.0f
        });

        stubNewScriptExecution(R_FAST_LANE, input);

        stubExecute(input);

        // mock the featureStatExecutorService
        doAnswer((it) -> {
            throw new RuntimeException("Some error during executing featureStat");
        }).when(featureStatExecutorService).executeFeatureStat(any(), any(), any());

        completeInputSuccessfully(input, "{\"output\": [2.0,4.0,6.0,10.0,16.0]}");

        var resultSet = protocolExecutorService.execute(1, 1, 4).resultSet().get();
        Assertions.assertEquals(StatusCode.FAILURE, resultSet.getOutcome());
        Assertions.assertEquals(0L, resultSet.getId());
        Assertions.assertEquals(1L, resultSet.getProtocolId());
        Assertions.assertEquals(4L, resultSet.getMeasId());
        Assertions.assertEquals(1L, resultSet.getPlateId());
        Assertions.assertEquals(1, resultSet.getErrors().size());
        Assertions.assertNotNull(resultSet.getErrorsText());

        var result1 = resultDataServiceClient.getResultData(0, 1);
        Assertions.assertArrayEquals(new float[]{2.0f, 4.0f, 6.0f, 10.0f, 16.0f}, result1.getValues());
        Assertions.assertEquals(StatusCode.SUCCESS, result1.getStatusCode());

        var error = resultSet.getErrors().get(0);
        Assertions.assertEquals("executing protocol => waiting for calculations of featureStats of a feature to complete => exception during execution", error.getDescription());
        Assertions.assertEquals("RuntimeException", error.getExceptionClassName());
        Assertions.assertEquals("Some error during executing featureStat", error.getExceptionMessage());
        Assertions.assertEquals(1, error.getFeatureId());

        verifyNoMoreInteractions(protocolInfoCollector, measurementServiceClient, scriptEngineClient);
    }

    @Test
    public void singleFeatureRetryThreeTimesTest() throws Exception {
        var formula = "output <- input$abc * 2";
        var input = new ScriptExecution(new TargetRuntime("R", "fast-lane", "v1"), formula,
                "{\"abc\":[1.0,2.0,3.0,5.0,8.0]}",
                "CalculationService"
        );

        var input2 = new ScriptExecution(new TargetRuntime("R", "fast-lane", "v1"), formula,
                "{\"abc\":[1.0,2.0,3.0,5.0,8.0]}",
                "CalculationService"
        );

        var input3 = new ScriptExecution(new TargetRuntime("R", "fast-lane", "v1"), formula,
                "{\"abc\":[1.0,2.0,3.0,5.0,8.0]}",
                "CalculationService"
        );

        stubGetProtocol(new Protocol(1L, "TestProtocol", null, true, true, "lc", "hc",
                new HashMap<>() {{
                    put(0, new Sequence(0, List.of(new Feature(1L, "Feature1", null, null, "AFormat", FeatureType.CALCULATION, 0,
                            new Formula(1L, "abc_duplicator", null, Category.CALCULATION, formula, ScriptLanguage.R, CalculationScope.WELL, "me", LocalDateTime.now(), "me", LocalDateTime.now()),
                            List.of(new CalculationInputValue(1L, 1L, "abc", null, "abc")), Collections.emptyList()))));
                }}));

        stubGetWellData(4L, "abc", new float[]{1.0f, 2.0f, 3.0f, 5.0f, 8.0f});

        doReturn(input, input2, input3).when(scriptEngineClient).newScriptExecution(R_FAST_LANE, input.getScriptExecutionInput().getScript(), input.getScriptExecutionInput().getInput());

        // attempt 1
        stubExecute(input);
        input.getOutput().complete(new ScriptExecutionOutputDTO(input.getScriptExecutionInput().getId(), "", ResponseStatusCode.WORKER_INTERNAL_ERROR, "Internal worker error", 0));

        // attempt  2
        stubExecute(input2);
        input2.getOutput().complete(new ScriptExecutionOutputDTO(input.getScriptExecutionInput().getId(), "", ResponseStatusCode.RESCHEDULED_BY_WATCHDOG, "Rescheduled by watchdog", 0));

        // attempt  3
        stubExecute(input3);
        completeInputSuccessfully(input3, "{\"output\": [2.0,4.0,6.0,10.0,16.0]}");

        stubExecuteFeatureStat();

        var resultSet = protocolExecutorService.execute(1, 1, 4).resultSet().get();
        Assertions.assertEquals(StatusCode.SUCCESS, resultSet.getOutcome());
        Assertions.assertEquals(0L, resultSet.getId());
        Assertions.assertEquals(1L, resultSet.getProtocolId());
        Assertions.assertEquals(4L, resultSet.getMeasId());
        Assertions.assertEquals(1L, resultSet.getPlateId());
        Assertions.assertEquals(Collections.emptyList(), resultSet.getErrors());
        Assertions.assertEquals("", resultSet.getErrorsText());

        var result1 = resultDataServiceClient.getResultData(0, 1);
        Assertions.assertArrayEquals(new float[]{2.0f, 4.0f, 6.0f, 10.0f, 16.0f}, result1.getValues());
        Assertions.assertEquals(StatusCode.SUCCESS, result1.getStatusCode());

        verifyNoMoreInteractions(protocolInfoCollector, measurementServiceClient, scriptEngineClient);
    }

    @Test
    public void singleFeatureRetryFrourTimesTest() throws Exception {
        var formula = "output <- input$abc * 2";
        var input = new ScriptExecution(new TargetRuntime("R", "fast-lane", "v1"), formula,
                "{\"abc\":[1.0,2.0,3.0,5.0,8.0]}",
                "CalculationService"
        );

        var input2 = new ScriptExecution(new TargetRuntime("R", "fast-lane", "v1"), formula,
                "{\"abc\":[1.0,2.0,3.0,5.0,8.0]}",
                "CalculationService"
        );

        var input3 = new ScriptExecution(new TargetRuntime("R", "fast-lane", "v1"), formula,
                "{\"abc\":[1.0,2.0,3.0,5.0,8.0]}",
                "CalculationService"
        );

        stubGetProtocol(new Protocol(1L, "TestProtocol", null, true, true, "lc", "hc",
                new HashMap<>() {{
                    put(0, new Sequence(0, List.of(new Feature(1L, "Feature1", null, null, "AFormat", FeatureType.CALCULATION, 0,
                            new Formula(1L, "abc_duplicator", null, Category.CALCULATION, formula, ScriptLanguage.R, CalculationScope.WELL, "me", LocalDateTime.now(), "me", LocalDateTime.now()),
                            List.of(new CalculationInputValue(1L, 1L, "abc", null, "abc")), Collections.emptyList()))));
                }}));

        stubGetWellData(4L, "abc", new float[]{1.0f, 2.0f, 3.0f, 5.0f, 8.0f});

        doReturn(input, input2, input3).when(scriptEngineClient).newScriptExecution(eq(R_FAST_LANE), eq(input.getScriptExecutionInput().getScript()), any(String.class));
        
        // attempt 1
        stubExecute(input);
        input.getOutput().complete(new ScriptExecutionOutputDTO(input.getScriptExecutionInput().getId(), "", ResponseStatusCode.WORKER_INTERNAL_ERROR, "Internal worker error", 0));

        // attempt  2
        stubExecute(input2);
        input2.getOutput().complete(new ScriptExecutionOutputDTO(input.getScriptExecutionInput().getId(), "", ResponseStatusCode.RESCHEDULED_BY_WATCHDOG, "Rescheduled by watchdog", 0));

        // attempt  3
        stubExecute(input3);
        input3.getOutput().complete(new ScriptExecutionOutputDTO(input.getScriptExecutionInput().getId(), "", ResponseStatusCode.WORKER_INTERNAL_ERROR, "Internal worker error", 0));

        var resultSet = protocolExecutorService.execute(1, 1, 4).resultSet().get();
        Assertions.assertEquals(StatusCode.FAILURE, resultSet.getOutcome());
        Assertions.assertEquals(0L, resultSet.getId());
        Assertions.assertEquals(1L, resultSet.getProtocolId());
        Assertions.assertEquals(4L, resultSet.getMeasId());
        Assertions.assertEquals(1L, resultSet.getPlateId());
        Assertions.assertEquals(1, resultSet.getErrors().size());
        Assertions.assertNotNull(resultSet.getErrorsText());

        var error = resultSet.getErrors().get(0);
        Assertions.assertEquals("executing sequence => processing output => output indicates error [WORKER_INTERNAL_ERROR]", error.getDescription());
        Assertions.assertNull(error.getExceptionClassName());
        Assertions.assertNull(error.getExceptionMessage());
        Assertions.assertEquals(1, error.getFeatureId());
        Assertions.assertEquals("Feature1", error.getFeatureName());
        Assertions.assertEquals(0, error.getSequenceNumber());
        Assertions.assertEquals(1, error.getFormulaId());
        Assertions.assertEquals("abc_duplicator", error.getFormulaName());
        Assertions.assertNull(error.getCivType());
        Assertions.assertEquals(0, error.getExitCode());
        Assertions.assertEquals("Internal worker error", error.getStatusMessage());

        var result1 = resultDataServiceClient.getResultData(0, 1);
        Assertions.assertEquals(0, result1.getValues().length);
        Assertions.assertEquals(StatusCode.FAILURE, result1.getStatusCode());

        verifyNoMoreInteractions(protocolInfoCollector, measurementServiceClient, scriptEngineClient);
    }

    private void stubGetProtocol(Protocol protocol) throws ProtocolUnresolvableException {
        doReturn(protocol)
                .when(protocolInfoCollector)
                .getProtocol(protocol.getId());
    }

    private void stubExecute(ScriptExecution input) throws JsonProcessingException {
        doNothing().when(scriptEngineClient).execute(input);
    }

    private void stubExecuteFeatureStat() {
        doReturn(true).when(featureStatExecutorService).executeFeatureStat(any(), any(), any());
    }

    private void stubExecuteWithExceptionAndDelay(ScriptExecution input, Throwable ex, long delay) throws
            JsonProcessingException {
        doAnswer(invocation -> {
            Thread.sleep(delay);
            throw ex;
        }).when(scriptEngineClient).execute(input);
    }

    private void stubGetWellData(Long measId, String columnName, float[] values) throws MeasUnresolvableException {
        doReturn(values).when(measurementServiceClient).getWellData(measId, columnName);
    }

    private void stubNewScriptExecution(String targetName, ScriptExecution scriptExecution) {
        doReturn(scriptExecution).when(scriptEngineClient).newScriptExecution(
        		eq(targetName), eq(scriptExecution.getScriptExecutionInput().getScript()), any(String.class));
    }

    private void completeInputSuccessfully(ScriptExecution input, String output) {
        input.getOutput().complete(new ScriptExecutionOutputDTO(input.getScriptExecutionInput().getId(), output, ResponseStatusCode.SUCCESS, "Ok", 0));
    }

    private void completeInputSuccessfullyWithDelay(ScriptExecution scriptExecution, String output, long delay) {
        new Timer().schedule(new TimerTask() {
            @Override
            public void run() {
                scriptExecution.getOutput().complete(new ScriptExecutionOutputDTO(scriptExecution.getScriptExecutionInput().getId(), output, ResponseStatusCode.SUCCESS, "Ok", 0));
            }
        }, delay);
    }

    private void completeInputWithExceptionAndDelay(ScriptExecution scriptExecution, Throwable ex, long delay) {
        new Timer().schedule(new TimerTask() {
            @Override
            public void run() {
                scriptExecution.getOutput().completeExceptionally(ex);
            }
        }, delay);
    }

    private void completeInputScriptError(ScriptExecution scriptExecution) {
        scriptExecution.getOutput().complete(new ScriptExecutionOutputDTO(scriptExecution.getScriptExecutionInput().getId(), "bogus!", ResponseStatusCode.SCRIPT_ERROR, "Script did not create output file!", 42));
    }

}
