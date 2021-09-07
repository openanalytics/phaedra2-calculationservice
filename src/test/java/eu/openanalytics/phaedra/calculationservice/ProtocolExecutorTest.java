package eu.openanalytics.phaedra.calculationservice;


import com.fasterxml.jackson.core.JsonProcessingException;
import eu.openanalytics.phaedra.calculationservice.controller.clients.MeasServiceClient;
import eu.openanalytics.phaedra.calculationservice.controller.clients.MeasUnresolvableException;
import eu.openanalytics.phaedra.calculationservice.controller.clients.ProtocolServiceClient;
import eu.openanalytics.phaedra.calculationservice.controller.clients.ProtocolUnresolvableException;
import eu.openanalytics.phaedra.calculationservice.enumeration.CalculationScope;
import eu.openanalytics.phaedra.calculationservice.enumeration.Category;
import eu.openanalytics.phaedra.calculationservice.enumeration.FeatureType;
import eu.openanalytics.phaedra.calculationservice.enumeration.ScriptLanguage;
import eu.openanalytics.phaedra.calculationservice.model.CalculationInputValue;
import eu.openanalytics.phaedra.calculationservice.model.Feature;
import eu.openanalytics.phaedra.calculationservice.model.Formula;
import eu.openanalytics.phaedra.calculationservice.model.Protocol;
import eu.openanalytics.phaedra.calculationservice.model.Sequence;
import eu.openanalytics.phaedra.calculationservice.service.FeatureExecutorService;
import eu.openanalytics.phaedra.calculationservice.service.ModelMapper;
import eu.openanalytics.phaedra.calculationservice.service.ProtocolExecutorService;
import eu.openanalytics.phaedra.calculationservice.service.SequenceExecutorService;
import eu.openanalytics.phaedra.resultdataservice.client.ResultDataServiceClient;
import eu.openanalytics.phaedra.resultdataservice.client.exception.ResultDataUnresolvableException;
import eu.openanalytics.phaedra.resultdataservice.enumeration.StatusCode;
import eu.openanalytics.phaedra.scriptengine.client.ScriptEngineClient;
import eu.openanalytics.phaedra.scriptengine.client.model.ScriptExecution;
import eu.openanalytics.phaedra.scriptengine.client.model.TargetRuntime;
import eu.openanalytics.phaedra.scriptengine.dto.ResponseStatusCode;
import eu.openanalytics.phaedra.scriptengine.dto.ScriptExecutionOutputDTO;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

import static eu.openanalytics.phaedra.calculationservice.CalculationService.R_FAST_LANE;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoMoreInteractions;

@MockitoSettings(strictness = Strictness.STRICT_STUBS)
@ExtendWith(MockitoExtension.class)
public class ProtocolExecutorTest {

    private <T> T mockUnimplemented(Class<T> clazz) {
        return mock(clazz, invocation -> {
            throw new IllegalStateException(String.format("[%s:%s] must be stubbed with arguments [%s]!", invocation.getMock().getClass().getSimpleName(), invocation.getMethod().getName(), Arrays.toString(invocation.getArguments())));
        });
    }

    private ProtocolServiceClient protocolServiceClient;
    private ResultDataServiceClient resultDataServiceClient;
    private MeasServiceClient measServiceClient;
    private ScriptEngineClient scriptEngineClient;

    private FeatureExecutorService featureExecutorService;
    private SequenceExecutorService sequenceExecutorService;
    private ProtocolExecutorService protocolExecutorService;
    private final ModelMapper modelMapper = new ModelMapper();

    @BeforeEach
    public void before() {
        protocolServiceClient = mockUnimplemented(ProtocolServiceClient.class);
        resultDataServiceClient = new InMemoryResultDataServiceClient();
        measServiceClient = mockUnimplemented(MeasServiceClient.class);
        scriptEngineClient = mockUnimplemented(ScriptEngineClient.class);
        featureExecutorService = new FeatureExecutorService(scriptEngineClient, measServiceClient, resultDataServiceClient);
        sequenceExecutorService = new SequenceExecutorService(resultDataServiceClient, featureExecutorService, modelMapper);
        protocolExecutorService = new ProtocolExecutorService(protocolServiceClient, resultDataServiceClient, sequenceExecutorService);
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
                            List.of(new CalculationInputValue(1L, 1L, "abc", null, "abc"))))));
                }}));

        stubGetWellData(4L, "abc", new float[]{1.0f, 2.0f, 3.0f, 5.0f, 8.0f});
        stubNewScriptExecution(R_FAST_LANE, input);
        stubExecute(input);
        completeInputSuccessfully(input, "{\"output\": [2.0,4.0,6.0,10.0,16.0]}");

        var resultSet = protocolExecutorService.execute(1, 1, 4).get();
        Assertions.assertEquals("Completed", resultSet.getOutcome());
        Assertions.assertEquals(0L, resultSet.getId());
        Assertions.assertEquals(1L, resultSet.getProtocolId());
        Assertions.assertEquals(4L, resultSet.getMeasId());
        Assertions.assertEquals(1L, resultSet.getPlateId());
        Assertions.assertEquals(Collections.emptyList(), resultSet.getErrors());
        Assertions.assertEquals("", resultSet.getErrorsText());

        var result1 = resultDataServiceClient.getResultData(0, 1);
        Assertions.assertArrayEquals(new float[]{2.0f, 4.0f, 6.0f, 10.0f, 16.0f}, result1.getValues());
        Assertions.assertEquals(StatusCode.SUCCESS, result1.getStatusCode());

        verifyNoMoreInteractions(protocolServiceClient, measServiceClient, scriptEngineClient);
    }

    @Test
    public void getWellDataGivesErrorTest() throws Exception {
        var formula = "output <- input$abc * 2";

        stubGetProtocol(new Protocol(1L, "TestProtocol", null, true, true, "lc", "hc",
                new HashMap<>() {{
                    put(0, new Sequence(0, List.of(new Feature(1L, "Feature1", null, null, "AFormat", FeatureType.CALCULATION, 0,
                            new Formula(1L, "abc_duplicator", null, Category.CALCULATION, formula, ScriptLanguage.R, CalculationScope.WELL, "me", LocalDateTime.now(), "me", LocalDateTime.now()),
                            List.of(new CalculationInputValue(1L, 1L, "abc", null, "abc"))))));
                }}));

        doThrow(new MeasUnresolvableException("WellData not found")).when(measServiceClient).getWellData(4L, "abc");

        var resultSet = protocolExecutorService.execute(1, 1, 4).get();
        Assertions.assertEquals("Error", resultSet.getOutcome());
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

        verifyNoMoreInteractions(protocolServiceClient, measServiceClient, scriptEngineClient);
    }

    @Test
    public void getResultDataGivesError() throws Exception {
        var mockResultDataServiceClient = mockUnimplemented(ResultDataServiceClient.class);
        featureExecutorService = new FeatureExecutorService(scriptEngineClient, measServiceClient, mockResultDataServiceClient);
        sequenceExecutorService = new SequenceExecutorService(resultDataServiceClient, featureExecutorService, modelMapper);
        protocolExecutorService = new ProtocolExecutorService(protocolServiceClient, resultDataServiceClient, sequenceExecutorService);
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
                            List.of(new CalculationInputValue(1L, 1L, "abc", null, "abc"))))));
                    put(1, new Sequence(1, List.of(new Feature(2L, "Feature2", null, null, "AFormat", FeatureType.CALCULATION, 1,
                            new Formula(2L, "result_duplicator", null, Category.CALCULATION, formula2, ScriptLanguage.R, CalculationScope.WELL, "me", LocalDateTime.now(), "me", LocalDateTime.now()),
                            List.of(new CalculationInputValue(2L, 2L, null, 1L, "result"))))));
                }}));

        stubGetWellData(4L, "abc", new float[]{1.0f, 2.0f, 3.0f, 5.0f, 8.0f});
        stubNewScriptExecution(R_FAST_LANE, input);
        stubExecute(input);
        completeInputSuccessfully(input, "{\"output\": [2.0,4.0,6.0,10.0,16.0]}");

        doThrow(new ResultDataUnresolvableException("ResultData not found")).when(mockResultDataServiceClient).getResultData(0, 1);

        var resultSet = protocolExecutorService.execute(1, 1, 4).get();
        Assertions.assertEquals("Error", resultSet.getOutcome());
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

        verifyNoMoreInteractions(protocolServiceClient, measServiceClient, scriptEngineClient);
    }

    @Test
    public void duplicateVariableNameTest() throws Exception {
        var formula1 = "output <- input$abc * 2";

        stubGetProtocol(new Protocol(1L, "TestProtocol", null, true, true, "lc", "hc",
                new HashMap<>() {{
                    put(0, new Sequence(0, List.of(new Feature(1L, "Feature1", null, null, "AFormat", FeatureType.CALCULATION, 0,
                            new Formula(1L, "abc_duplicator", null, Category.CALCULATION, formula1, ScriptLanguage.R, CalculationScope.WELL, "me", LocalDateTime.now(), "me", LocalDateTime.now()),
                            List.of(new CalculationInputValue(1L, 1L, "abc", null, "abc"),
                                    new CalculationInputValue(1L, 1L, "xyz", null, "abc"))))));
                }}));

        stubGetWellData(4L, "abc", new float[]{1.0f, 2.0f, 3.0f, 5.0f, 8.0f});

        var resultSet = protocolExecutorService.execute(1, 1, 4).get();
        Assertions.assertEquals("Error", resultSet.getOutcome());
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

        verifyNoMoreInteractions(protocolServiceClient, measServiceClient, scriptEngineClient);
    }

    @Test
    public void variableMissingSourceTest() throws Exception {
        var formula1 = "output <- input$abc * 2";

        stubGetProtocol(new Protocol(1L, "TestProtocol", null, true, true, "lc", "hc",
                new HashMap<>() {{
                    put(0, new Sequence(0, List.of(new Feature(1L, "Feature1", null, null, "AFormat", FeatureType.CALCULATION, 0,
                            new Formula(1L, "abc_duplicator", null, Category.CALCULATION, formula1, ScriptLanguage.R, CalculationScope.WELL, "me", LocalDateTime.now(), "me", LocalDateTime.now()),
                            List.of(new CalculationInputValue(1L, 1L, null, null, "abc"))))));
                }}));

        var resultSet = protocolExecutorService.execute(1, 1, 4).get();
        Assertions.assertEquals("Error", resultSet.getOutcome());
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

        verifyNoMoreInteractions(protocolServiceClient, measServiceClient, scriptEngineClient);
    }

    @Test
    public void variableUsingFeatureInFirstSequenceTest() throws Exception {
        var formula1 = "output <- input$abc * 2";

        stubGetProtocol(new Protocol(1L, "TestProtocol", null, true, true, "lc", "hc",
                new HashMap<>() {{
                    put(0, new Sequence(0, List.of(new Feature(1L, "Feature1", null, null, "AFormat", FeatureType.CALCULATION, 0,
                            new Formula(1L, "abc_duplicator", null, Category.CALCULATION, formula1, ScriptLanguage.R, CalculationScope.WELL, "me", LocalDateTime.now(), "me", LocalDateTime.now()),
                            List.of(new CalculationInputValue(1L, 1L, null, 1L, "abc"))))));
                }}));

        var resultSet = protocolExecutorService.execute(1, 1, 4).get();
        Assertions.assertEquals("Error", resultSet.getOutcome());
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

        verifyNoMoreInteractions(protocolServiceClient, measServiceClient, scriptEngineClient);
    }

    @Test
    public void invalidNumberOfSequencesTest() throws Exception {
        stubGetProtocol(new Protocol(1L, "TestProtocol", null, true, true, "lc", "hc",
                new HashMap<>() {{
                    put(1, new Sequence(1, List.of(new Feature(1L, "Feature1", null, null, "AFormat", FeatureType.CALCULATION, 1,
                            new Formula(1L, "abc_duplicator", null, Category.CALCULATION, "bogus", ScriptLanguage.R, CalculationScope.WELL, "me", LocalDateTime.now(), "me", LocalDateTime.now()),
                            List.of(new CalculationInputValue(1L, 1L, "abc", null, "abc"))))));
                }}));

        var resultSet = protocolExecutorService.execute(1, 1, 4).get();
        Assertions.assertEquals("Error", resultSet.getOutcome());
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

        verifyNoMoreInteractions(protocolServiceClient, measServiceClient, scriptEngineClient);
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
                            List.of(new CalculationInputValue(1L, 1L, "abc", null, "abc"))))));
                }}));

        stubGetWellData(4L, "abc", new float[]{1.0f, 2.0f, 3.0f, 5.0f, 8.0f});
        stubNewScriptExecution(R_FAST_LANE, input);
        stubExecute(input);
        completeInputScriptError(input);

        var resultSet = protocolExecutorService.execute(1, 1, 4).get();
        Assertions.assertEquals("Error", resultSet.getOutcome());
        Assertions.assertEquals(0L, resultSet.getId());
        Assertions.assertEquals(1L, resultSet.getProtocolId());
        Assertions.assertEquals(4L, resultSet.getMeasId());
        Assertions.assertEquals(1L, resultSet.getPlateId());
        Assertions.assertEquals(1, resultSet.getErrors().size());
        Assertions.assertNotNull(resultSet.getErrorsText());

        var error = resultSet.getErrors().get(0);
        Assertions.assertEquals("executing sequence => processing output => output indicates script error", error.getDescription());
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
        Assertions.assertEquals(StatusCode.SCRIPT_ERROR, result1.getStatusCode());

        verifyNoMoreInteractions(protocolServiceClient, measServiceClient, scriptEngineClient);
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
                            List.of(new CalculationInputValue(1L, 1L, "abc", null, "abc"))))));
                }}));

        stubGetWellData(4L, "abc", new float[]{1.0f, 2.0f, 3.0f, 5.0f, 8.0f});
        stubNewScriptExecution(R_FAST_LANE, input);
        stubExecute(input);
        completeInputSuccessfully(input, "{\"output\": [2.0,4.0,6.0,10.0,16.0}"); // invalid output!

        var resultSet = protocolExecutorService.execute(1, 1, 4).get();
        Assertions.assertEquals("Error", resultSet.getOutcome());
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

        verifyNoMoreInteractions(protocolServiceClient, measServiceClient, scriptEngineClient);
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
                            List.of(new CalculationInputValue(1L, 1L, "abc", null, "abc"))))));
                }}));

        stubGetWellData(4L, "abc", new float[]{1.0f, 2.0f, 3.0f, 5.0f, 8.0f});
        stubNewScriptExecution(R_FAST_LANE, input);
        stubExecute(input);
        input.getOutput().completeExceptionally(new RuntimeException("Some error during execution!"));

        var resultSet = protocolExecutorService.execute(1, 1, 4).get();
        Assertions.assertEquals("Error", resultSet.getOutcome());
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

        verifyNoMoreInteractions(protocolServiceClient, measServiceClient, scriptEngineClient);
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
                                    List.of(new CalculationInputValue(1L, 1L, "abc", null, "abc"))),
                            new Feature(2L, "Feature2", null, null, "AFormat", FeatureType.CALCULATION, 0,
                                    new Formula(2L, "abc_times_three", null, Category.CALCULATION, formula2, ScriptLanguage.R, CalculationScope.WELL, "me", LocalDateTime.now(), "me", LocalDateTime.now()),
                                    List.of(new CalculationInputValue(2L, 2L, "abc", null, "abc"))),
                            new Feature(3L, "Feature2", null, null, "AFormat", FeatureType.CALCULATION, 0,
                                    new Formula(3L, "abc_times_four", null, Category.CALCULATION, formula3, ScriptLanguage.R, CalculationScope.WELL, "me", LocalDateTime.now(), "me", LocalDateTime.now()),
                                    List.of(new CalculationInputValue(3L, 3L, "abc", null, "abc"))))
                    ));
                }}));

        stubGetWellData(4L, "abc", new float[]{1.0f, 2.0f, 3.0f, 5.0f, 8.0f});

        stubNewScriptExecution(R_FAST_LANE, input1);
        stubNewScriptExecution(R_FAST_LANE, input2);
        stubNewScriptExecution(R_FAST_LANE, input3);
        stubExecute(input1);
        stubExecute(input2);
        stubExecute(input3);

        // input 1: return value after 100 ms
        completeInputSuccessfullyWithDelay(input1, "{\"output\": [2.0,4.0,6.0,10.0,16.0]}", 100);

        // input 2: return error after 250 ms
        completeInputWithExceptionAndDelay(input2, new RuntimeException("Some error during execution!"), 250);

        // input 3:  return value after 1000ms -> should NOT get cancelled
        completeInputSuccessfullyWithDelay(input3, "{\"output\": [4.0,8.0,12.0,20.0,32.0]}", 1000);

        var resultSet = protocolExecutorService.execute(1, 1, 4).get();

        Assertions.assertFalse(input1.getOutput().isCancelled());
        Assertions.assertFalse(input2.getOutput().isCancelled());
        Assertions.assertFalse(input3.getOutput().isCancelled());

        Assertions.assertEquals("Error", resultSet.getOutcome());
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

        verifyNoMoreInteractions(protocolServiceClient, measServiceClient, scriptEngineClient);
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
                                    List.of(new CalculationInputValue(1L, 1L, "abc", null, "abc"))),
                            new Feature(2L, "Feature2", null, null, "AFormat", FeatureType.CALCULATION, 0,
                                    new Formula(2L, "abc_times_three", null, Category.CALCULATION, formula2, ScriptLanguage.R, CalculationScope.WELL, "me", LocalDateTime.now(), "me", LocalDateTime.now()),
                                    List.of(new CalculationInputValue(2L, 2L, "abc", null, "abc"))),
                            new Feature(3L, "Feature2", null, null, "AFormat", FeatureType.CALCULATION, 0,
                                    new Formula(3L, "abc_times_four", null, Category.CALCULATION, formula3, ScriptLanguage.R, CalculationScope.WELL, "me", LocalDateTime.now(), "me", LocalDateTime.now()),
                                    List.of(new CalculationInputValue(3L, 3L, "abc", null, "abc"))))
                    ));
                }}));

        stubGetWellData(4L, "abc", new float[]{1.0f, 2.0f, 3.0f, 5.0f, 8.0f});

        stubNewScriptExecution(R_FAST_LANE, input1);
        stubNewScriptExecution(R_FAST_LANE, input2);
        stubNewScriptExecution(R_FAST_LANE, input3);
        stubExecute(input1);
        stubExecuteWithExceptionAndDelay(input2, new RuntimeException("Error during sending of execution!"), 1000);
        stubExecute(input3);

        // input 1: return value after 100 ms
        completeInputSuccessfullyWithDelay(input1, "{\"output\": [2.0,4.0,6.0,10.0,16.0]}", 100);

        // input 3:  return value after 1000ms -> should NOT get cancelled
        completeInputSuccessfullyWithDelay(input3, "{\"output\": [4.0,8.0,12.0,20.0,32.0]}", 1000);

        var resultSet = protocolExecutorService.execute(1, 1, 4).get();

        Assertions.assertFalse(input1.getOutput().isCancelled()); //
        Assertions.assertFalse(input2.getOutput().isCancelled());
        Assertions.assertFalse(input2.getOutput().isDone());
        Assertions.assertFalse(input2.getOutput().isCompletedExceptionally());
        Assertions.assertFalse(input3.getOutput().isCancelled());

        Assertions.assertEquals("Error", resultSet.getOutcome());
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

        verifyNoMoreInteractions(protocolServiceClient, measServiceClient, scriptEngineClient);
    }

    private void stubGetProtocol(Protocol protocol) throws ProtocolUnresolvableException {
        doReturn(protocol)
                .when(protocolServiceClient)
                .getProtocol(protocol.getId());
    }

    private void stubExecute(ScriptExecution input) throws JsonProcessingException {
        doNothing().when(scriptEngineClient).execute(input);
    }

    private void stubExecuteWithExceptionAndDelay(ScriptExecution input, Throwable ex, long delay) throws JsonProcessingException {
        doAnswer(invocation -> {
            Thread.sleep(delay);
            throw ex;
        }).when(scriptEngineClient).execute(input);
    }

    private void stubGetWellData(Long measId, String columnName, float[] values) throws MeasUnresolvableException {
        doReturn(values).when(measServiceClient).getWellData(measId, columnName);
    }

    private void stubNewScriptExecution(String targetName, ScriptExecution scriptExecution) {
        doReturn(scriptExecution).when(scriptEngineClient).newScriptExecution(targetName, scriptExecution.getScriptExecutionInput().getScript(), scriptExecution.getScriptExecutionInput().getInput());
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
