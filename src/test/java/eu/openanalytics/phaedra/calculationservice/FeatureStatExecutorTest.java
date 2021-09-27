package eu.openanalytics.phaedra.calculationservice;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.openanalytics.phaedra.calculationservice.enumeration.CalculationScope;
import eu.openanalytics.phaedra.calculationservice.enumeration.Category;
import eu.openanalytics.phaedra.calculationservice.enumeration.FeatureType;
import eu.openanalytics.phaedra.calculationservice.enumeration.ScriptLanguage;
import eu.openanalytics.phaedra.calculationservice.model.CalculationContext;
import eu.openanalytics.phaedra.calculationservice.model.CalculationInputValue;
import eu.openanalytics.phaedra.calculationservice.model.Feature;
import eu.openanalytics.phaedra.calculationservice.model.FeatureStat;
import eu.openanalytics.phaedra.calculationservice.model.Formula;
import eu.openanalytics.phaedra.calculationservice.model.Protocol;
import eu.openanalytics.phaedra.calculationservice.model.Sequence;
import eu.openanalytics.phaedra.calculationservice.service.ModelMapper;
import eu.openanalytics.phaedra.calculationservice.service.featurestat.FeatureStatExecutor;
import eu.openanalytics.phaedra.calculationservice.service.protocol.ErrorCollector;
import eu.openanalytics.phaedra.calculationservice.support.InMemoryResultDataServiceClient;
import eu.openanalytics.phaedra.platservice.client.PlateServiceClient;
import eu.openanalytics.phaedra.platservice.client.exception.PlateUnresolvableException;
import eu.openanalytics.phaedra.platservice.dto.PlateDTO;
import eu.openanalytics.phaedra.platservice.dto.WellDTO;
import eu.openanalytics.phaedra.platservice.enumartion.WellStatus;
import eu.openanalytics.phaedra.resultdataservice.client.ResultDataServiceClient;
import eu.openanalytics.phaedra.resultdataservice.client.exception.ResultFeatureStatUnresolvableException;
import eu.openanalytics.phaedra.resultdataservice.dto.ResultDataDTO;
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
import java.util.HashMap;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;

import static eu.openanalytics.phaedra.calculationservice.CalculationService.JAVASTAT_FAST_LANE;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoMoreInteractions;

@MockitoSettings(strictness = Strictness.STRICT_STUBS)
@ExtendWith(MockitoExtension.class)
public class FeatureStatExecutorTest {


    private <T> T mockUnimplemented(Class<T> clazz) {
        return mock(clazz, invocation -> {
            throw new IllegalStateException(String.format("[%s:%s] must be stubbed with arguments [%s]!", invocation.getMock().getClass().getSimpleName(), invocation.getMethod().getName(), Arrays.toString(invocation.getArguments())));
        });
    }

    private final ModelMapper modelMapper = new ModelMapper();
    private final ObjectMapper objectMapper = new ObjectMapper();

    private PlateServiceClient plateServiceClient;
    private FeatureStatExecutor featureStatExecutor;
    private ScriptEngineClient scriptEngineClient;
    private ResultDataServiceClient resultDataServiceClient;

    @BeforeEach
    public void before() {
        resultDataServiceClient = new InMemoryResultDataServiceClient(); // TODO mock or inMemoery?
        scriptEngineClient = mockUnimplemented(ScriptEngineClient.class);
        plateServiceClient = mockUnimplemented(PlateServiceClient.class);
        featureStatExecutor = new FeatureStatExecutor(plateServiceClient, scriptEngineClient, objectMapper, resultDataServiceClient, modelMapper);
    }

    @Test
    public void simpleTestWithSingleStat() throws Exception {
        var plate = PlateDTO.builder().id(10L).build();
        var formula = createFormula("JavaStat::count", "count", 13L);
        var featureStat = createFeatureStat(formula);

        var feature = new Feature(1L, "Feature1", null, null, "AFormat", FeatureType.CALCULATION, 0,
                new Formula(1L, "abc_duplicator", null, Category.CALCULATION, "output <- input$abc * 2", ScriptLanguage.R, CalculationScope.WELL, "me", LocalDateTime.now(), "me", LocalDateTime.now()),
                List.of(new CalculationInputValue(1L, 1L, "abc", null, "abc")), List.of(featureStat));

        var protocol = new Protocol(1L, "TestProtocol", null, true, true, "LC", "HCG",
                new HashMap<>() {{
                    put(0, new Sequence(0, List.of(feature)));
                }});
        var cctx = new CalculationContext(plate, protocol, 1L, 2L, new ErrorCollector(), new ConcurrentHashMap<>());

        var input = new ScriptExecution(new TargetRuntime("JAVASTAT", "fast-lane", "v1"), formula.getFormula(),
                "{\"isWelltypeStat\":true,\"isPlateStat\":true,\"welltypes\":[\"LC\",\"SAMPLE\",\"SAMPLE\",\"HC\"],\"highWelltype\":\"HCG\",\"lowWelltype\":\"LC\",\"featureValues\":[1.0,2.0,3.0,5.0]}",
                "CalculationService"
        );

        stubGetWells();
        stubNewScriptExecution(input);
        stubExecute(input);
        completeInputSuccessfully(input, "{\"plateValue\": 42, \"welltypeValues\": {\"HC\": 10, \"LC\": 43, \"SAMPLE\": 52}}");

        var success = featureStatExecutor.executeFeatureStat(cctx, feature, createResultData());

        Assertions.assertTrue(success);

        assertPlateFeatureStatResult(0L, "count", 42);
        assertFeatureStatResult("HC", 1L, "count", 10);
        assertFeatureStatResult("LC", 2L, "count", 43);
        assertFeatureStatResult("SAMPLE", 3L, "count", 52);

        verifyNoMoreInteractions(plateServiceClient, scriptEngineClient);
    }

    @Test
    public void simpleTestWitMultipleStats() throws Exception {
        var plate = PlateDTO.builder().id(10L).build();

        var formula1 = createFormula("JavaStat::count", "count", 13L);
        var featureStat1 = createFeatureStat(formula1);

        var formula2 = createFormula("JavaStat::min", "min", 14L);
        var featureStat2 = createFeatureStat(formula2);

        var formula3 = createFormula("JavaStat::max", "max", 14L);
        var featureStat3 = createFeatureStat(formula3);

        var formula4 = createFormula("JavaStat::zprime", "zprime", 15L);
        var featureStat4 = createFeatureStat(formula4, false);

        var feature = new Feature(1L, "Feature1", null, null, "AFormat", FeatureType.CALCULATION, 0,
                new Formula(1L, "abc_duplicator", null, Category.CALCULATION, "output <- input$abc * 2", ScriptLanguage.R, CalculationScope.WELL, "me", LocalDateTime.now(), "me", LocalDateTime.now()),
                List.of(new CalculationInputValue(1L, 1L, "abc", null, "abc")), List.of(featureStat1, featureStat2, featureStat3, featureStat4));

        var protocol = new Protocol(1L, "TestProtocol", null, true, true, "LC", "HCG",
                new HashMap<>() {{
                    put(0, new Sequence(0, List.of(feature)));
                }});
        var cctx = new CalculationContext(plate, protocol, 1L, 2L, new ErrorCollector(), new ConcurrentHashMap<>());

        var input1 = new ScriptExecution(new TargetRuntime("JAVASTAT", "fast-lane", "v1"), formula1.getFormula(),
                "{\"isWelltypeStat\":true,\"isPlateStat\":true,\"welltypes\":[\"LC\",\"SAMPLE\",\"SAMPLE\",\"HC\"],\"highWelltype\":\"HCG\",\"lowWelltype\":\"LC\",\"featureValues\":[1.0,2.0,3.0,5.0]}",
                "CalculationService"
        );

        var input2 = new ScriptExecution(new TargetRuntime("JAVASTAT", "fast-lane", "v1"), formula2.getFormula(),
                "{\"isWelltypeStat\":true,\"isPlateStat\":true,\"welltypes\":[\"LC\",\"SAMPLE\",\"SAMPLE\",\"HC\"],\"highWelltype\":\"HCG\",\"lowWelltype\":\"LC\",\"featureValues\":[1.0,2.0,3.0,5.0]}",
                "CalculationService"
        );

        var input3 = new ScriptExecution(new TargetRuntime("JAVASTAT", "fast-lane", "v1"), formula3.getFormula(),
                "{\"isWelltypeStat\":true,\"isPlateStat\":true,\"welltypes\":[\"LC\",\"SAMPLE\",\"SAMPLE\",\"HC\"],\"highWelltype\":\"HCG\",\"lowWelltype\":\"LC\",\"featureValues\":[1.0,2.0,3.0,5.0]}",
                "CalculationService"
        );

        var input4 = new ScriptExecution(new TargetRuntime("JAVASTAT", "fast-lane", "v1"), formula4.getFormula(),
                "{\"isWelltypeStat\":false,\"isPlateStat\":true,\"welltypes\":[\"LC\",\"SAMPLE\",\"SAMPLE\",\"HC\"],\"highWelltype\":\"HCG\",\"lowWelltype\":\"LC\",\"featureValues\":[1.0,2.0,3.0,5.0]}",
                "CalculationService"
        );

        stubGetWells();
        stubNewScriptExecution(input1);
        stubExecute(input1);
        completeInputSuccessfully(input1, "{\"plateValue\": 42, \"welltypeValues\": {\"HC\": 10, \"LC\": 50, \"SAMPLE\": 61}}");

        stubNewScriptExecution(input2);
        stubExecute(input2);
        completeInputSuccessfully(input2, "{\"plateValue\": 43, \"welltypeValues\": {\"HC\": 11, \"LC\": 51, \"SAMPLE\": 62}}");

        stubNewScriptExecution(input3);
        stubExecute(input3);
        completeInputSuccessfully(input3, "{\"plateValue\": 44, \"welltypeValues\": {\"HC\": 12, \"LC\": 52, \"SAMPLE\": 63}}");

        stubNewScriptExecution(input4);
        stubExecute(input4);
        completeInputSuccessfully(input4, "{\"plateValue\": 45, \"welltypeValues\": {}}");

        var success = featureStatExecutor.executeFeatureStat(cctx, feature, createResultData());

        Assertions.assertTrue(success);

        assertPlateFeatureStatResult(0L, "count", 42);
        assertFeatureStatResult("HC", 1L, "count", 10);
        assertFeatureStatResult("LC", 2L, "count", 50);
        assertFeatureStatResult("SAMPLE", 3L, "count", 61);

        assertPlateFeatureStatResult(4L, "min", 43);
        assertFeatureStatResult("HC", 5L, "min", 11);
        assertFeatureStatResult("LC", 6L, "min", 51);
        assertFeatureStatResult("SAMPLE", 7L, "min", 62);

        assertPlateFeatureStatResult(8L, "max", 44);
        assertFeatureStatResult("HC", 9L, "max", 12);
        assertFeatureStatResult("LC", 10L, "max", 52);
        assertFeatureStatResult("SAMPLE", 11L, "max", 63);

        assertPlateFeatureStatResult(12L, "zprime", 45);

        verifyNoMoreInteractions(plateServiceClient, scriptEngineClient);
    }

    @Test
    public void testInvalidFeatureId() {
        var cctx = new CalculationContext(null, null, 1L, 2L, new ErrorCollector(), new ConcurrentHashMap<>());

        var feature = new Feature(1L, "Feature1", null, null, "AFormat", FeatureType.CALCULATION, 0,
                new Formula(1L, "abc_duplicator", null, Category.CALCULATION, "output <- input$abc * 2", ScriptLanguage.R, CalculationScope.WELL, "me", LocalDateTime.now(), "me", LocalDateTime.now()),
                List.of(new CalculationInputValue(1L, 1L, "abc", null, "abc")), List.of());

        var resultData = ResultDataDTO.builder()
                .featureId(42L) // wrong -> should give error
                .id(25L)
                .exitCode(0)
                .statusCode(StatusCode.SUCCESS)
                .statusMessage("Ok")
                .resultSetId(25L)
                .values(new float[]{1.0f, 2.0f, 3.0f, 5.0f})
                .createdTimestamp(LocalDateTime.now())
                .build();

        var success = featureStatExecutor.executeFeatureStat(cctx, feature, resultData);
        Assertions.assertFalse(success);
        Assertions.assertTrue(cctx.errorCollector().hasError());
        Assertions.assertEquals(1, cctx.errorCollector().getErrors().size());
        Assertions.assertEquals("Skipping calculating FeatureStats because FeatureId does not match the FeatureId of the ResultData", cctx.errorCollector().getErrors().get(0).getDescription());
        Assertions.assertEquals(1L, cctx.errorCollector().getErrors().get(0).getFeatureId());
    }


    @Test
    public void testInvalidStatusCode() {
        var cctx = new CalculationContext(null, null, 1L, 2L, new ErrorCollector(), new ConcurrentHashMap<>());

        var feature = new Feature(1L, "Feature1", null, null, "AFormat", FeatureType.CALCULATION, 0,
                new Formula(1L, "abc_duplicator", null, Category.CALCULATION, "output <- input$abc * 2", ScriptLanguage.R, CalculationScope.WELL, "me", LocalDateTime.now(), "me", LocalDateTime.now()),
                List.of(new CalculationInputValue(1L, 1L, "abc", null, "abc")), List.of());

        var resultData = ResultDataDTO.builder()
                .featureId(1L)
                .id(25L)
                .exitCode(0)
                .statusCode(StatusCode.SCRIPT_ERROR) // should give an error
                .statusMessage("Ok")
                .resultSetId(25L)
                .values(new float[]{1.0f, 2.0f, 3.0f, 5.0f})
                .createdTimestamp(LocalDateTime.now())
                .build();

        var success = featureStatExecutor.executeFeatureStat(cctx, feature, resultData);
        Assertions.assertFalse(success);
        Assertions.assertTrue(cctx.errorCollector().hasError());
        Assertions.assertEquals(1, cctx.errorCollector().getErrors().size());
        Assertions.assertEquals("Skipping calculating FeatureStats because the ResultData indicates an error", cctx.errorCollector().getErrors().get(0).getDescription());
        Assertions.assertEquals(1L, cctx.errorCollector().getErrors().get(0).getFeatureId());
    }

    @Test
    public void testInvalidFormula() throws Exception {
        var plate = PlateDTO.builder().id(10L).build();

        var formula1 = createFormula("JavaStat::count", "count", 13L);
        var featureStat1 = createFeatureStat(formula1);

        var formula2 = Formula.builder()
                .formula("bogus")
                .category(Category.CALCULATION)
                .name("min")
                .id(14L)
                .language(ScriptLanguage.JAVASCRIPT)
                .scope(CalculationScope.PLATE)
                .build();
        var featureStat2 = createFeatureStat(formula2);

        var feature = new Feature(1L, "Feature1", null, null, "AFormat", FeatureType.CALCULATION, 0,
                new Formula(1L, "abc_duplicator", null, Category.CALCULATION, "output <- input$abc * 2", ScriptLanguage.R, CalculationScope.WELL, "me", LocalDateTime.now(), "me", LocalDateTime.now()),
                List.of(new CalculationInputValue(1L, 1L, "abc", null, "abc")), List.of(featureStat1, featureStat2));

        var protocol = new Protocol(1L, "TestProtocol", null, true, true, "LC", "HCG",
                new HashMap<>() {{
                    put(0, new Sequence(0, List.of(feature)));
                }});
        var cctx = new CalculationContext(plate, protocol, 1L, 2L, new ErrorCollector(), new ConcurrentHashMap<>());

        var input1 = new ScriptExecution(new TargetRuntime("JAVASTAT", "fast-lane", "v1"), formula1.getFormula(),
                "{\"isWelltypeStat\":true,\"isPlateStat\":true,\"welltypes\":[\"LC\",\"SAMPLE\",\"SAMPLE\",\"HC\"],\"highWelltype\":\"HCG\",\"lowWelltype\":\"LC\",\"featureValues\":[1.0,2.0,3.0,5.0]}",
                "CalculationService"
        );

        stubGetWells();
        stubNewScriptExecution(input1);
        stubExecute(input1);
        completeInputSuccessfully(input1, "{\"plateValue\": 42, \"welltypeValues\": {\"HC\": 10, \"LC\": 50, \"SAMPLE\": 61}}");

        var success = featureStatExecutor.executeFeatureStat(cctx, feature, createResultData());

        Assertions.assertFalse(success);

        assertPlateFeatureStatResult(0L, "count", 42);
        assertFeatureStatResult("HC", 1L, "count", 10);
        assertFeatureStatResult("LC", 2L, "count", 50);
        assertFeatureStatResult("SAMPLE", 3L, "count", 61);

        Assertions.assertTrue(cctx.errorCollector().hasError());
        Assertions.assertEquals(1, cctx.errorCollector().getErrors().size());
        Assertions.assertEquals("Skipping calculating FeatureStat because the formula is not invalid (category must be CALCULATION, language must be JAVASTAT)", cctx.errorCollector().getErrors().get(0).getDescription());
        Assertions.assertEquals(1L, cctx.errorCollector().getErrors().get(0).getFeatureId());
        Assertions.assertEquals(14L, cctx.errorCollector().getErrors().get(0).getFormulaId());
        Assertions.assertEquals("min", cctx.errorCollector().getErrors().get(0).getFormulaName());

        verifyNoMoreInteractions(plateServiceClient, scriptEngineClient);
    }

    @Test
    public void exceptionDuringExecutionTest() throws Exception {
        var plate = PlateDTO.builder().id(10L).build();

        var formula1 = createFormula("JavaStat::count", "count", 13L);
        var featureStat1 = createFeatureStat(formula1);

        // featureStat2 will cause an exception
        var formula2 = createFormula("JavaStat::min", "min", 14L);
        var featureStat2 = createFeatureStat(formula2);

        var feature = new Feature(1L, "Feature1", null, null, "AFormat", FeatureType.CALCULATION, 0,
                new Formula(1L, "abc_duplicator", null, Category.CALCULATION, "output <- input$abc * 2", ScriptLanguage.R, CalculationScope.WELL, "me", LocalDateTime.now(), "me", LocalDateTime.now()),
                List.of(new CalculationInputValue(1L, 1L, "abc", null, "abc")), List.of(featureStat1, featureStat2));

        var protocol = new Protocol(1L, "TestProtocol", null, true, true, "LC", "HCG",
                new HashMap<>() {{
                    put(0, new Sequence(0, List.of(feature)));
                }});
        var cctx = new CalculationContext(plate, protocol, 1L, 2L, new ErrorCollector(), new ConcurrentHashMap<>());

        var input1 = new ScriptExecution(new TargetRuntime("JAVASTAT", "fast-lane", "v1"), formula1.getFormula(),
                "{\"isWelltypeStat\":true,\"isPlateStat\":true,\"welltypes\":[\"LC\",\"SAMPLE\",\"SAMPLE\",\"HC\"],\"highWelltype\":\"HCG\",\"lowWelltype\":\"LC\",\"featureValues\":[1.0,2.0,3.0,5.0]}",
                "CalculationService"
        );

        var input2 = new ScriptExecution(new TargetRuntime("JAVASTAT", "fast-lane", "v1"), formula2.getFormula(),
                "{\"isWelltypeStat\":true,\"isPlateStat\":true,\"welltypes\":[\"LC\",\"SAMPLE\",\"SAMPLE\",\"HC\"],\"highWelltype\":\"HCG\",\"lowWelltype\":\"LC\",\"featureValues\":[1.0,2.0,3.0,5.0]}",
                "CalculationService"
        );

        stubGetWells();
        stubNewScriptExecution(input1);
        stubExecute(input1);
        completeInputSuccessfully(input1, "{\"plateValue\": 42, \"welltypeValues\": {\"HC\": 10, \"LC\": 50, \"SAMPLE\": 61}}");

        stubNewScriptExecution(input2);
        stubExecute(input2);
        input2.getOutput().completeExceptionally(new RuntimeException("Some error during execution!"));

        var success = featureStatExecutor.executeFeatureStat(cctx, feature, createResultData());

        Assertions.assertFalse(success);

        assertPlateFeatureStatResult(0L, "count", 42);
        assertFeatureStatResult("HC", 1L, "count", 10);
        assertFeatureStatResult("LC", 2L, "count", 50);
        assertFeatureStatResult("SAMPLE", 3L, "count", 61);

        Assertions.assertTrue(cctx.errorCollector().hasError());
        Assertions.assertEquals(1, cctx.errorCollector().getErrors().size());
        Assertions.assertEquals("executing featureStat => waiting for output to be received => exception during execution", cctx.errorCollector().getErrors().get(0).getDescription());
        Assertions.assertEquals("Some error during execution!", cctx.errorCollector().getErrors().get(0).getExceptionMessage());
        Assertions.assertEquals("RuntimeException", cctx.errorCollector().getErrors().get(0).getExceptionClassName());
        Assertions.assertEquals(1L, cctx.errorCollector().getErrors().get(0).getFeatureId());
        Assertions.assertEquals(14L, cctx.errorCollector().getErrors().get(0).getFormulaId());
        Assertions.assertEquals("min", cctx.errorCollector().getErrors().get(0).getFormulaName());

        verifyNoMoreInteractions(plateServiceClient, scriptEngineClient);
    }

    @Test
    public void futureCancelledDuringExecutionTest() throws Exception {
        var plate = PlateDTO.builder().id(10L).build();

        var formula1 = createFormula("JavaStat::count", "count", 13L);
        var featureStat1 = createFeatureStat(formula1);

        // featureStat2 will cause an exception
        var formula2 = createFormula("JavaStat::min", "min", 14L);
        var featureStat2 = createFeatureStat(formula2);

        var feature = new Feature(1L, "Feature1", null, null, "AFormat", FeatureType.CALCULATION, 0,
                new Formula(1L, "abc_duplicator", null, Category.CALCULATION, "output <- input$abc * 2", ScriptLanguage.R, CalculationScope.WELL, "me", LocalDateTime.now(), "me", LocalDateTime.now()),
                List.of(new CalculationInputValue(1L, 1L, "abc", null, "abc")), List.of(featureStat1, featureStat2));

        var protocol = new Protocol(1L, "TestProtocol", null, true, true, "LC", "HCG",
                new HashMap<>() {{
                    put(0, new Sequence(0, List.of(feature)));
                }});
        var cctx = new CalculationContext(plate, protocol, 1L, 2L, new ErrorCollector(), new ConcurrentHashMap<>());

        var input1 = new ScriptExecution(new TargetRuntime("JAVASTAT", "fast-lane", "v1"), formula1.getFormula(),
                "{\"isWelltypeStat\":true,\"isPlateStat\":true,\"welltypes\":[\"LC\",\"SAMPLE\",\"SAMPLE\",\"HC\"],\"highWelltype\":\"HCG\",\"lowWelltype\":\"LC\",\"featureValues\":[1.0,2.0,3.0,5.0]}",
                "CalculationService"
        );

        var input2 = new ScriptExecution(new TargetRuntime("JAVASTAT", "fast-lane", "v1"), formula2.getFormula(),
                "{\"isWelltypeStat\":true,\"isPlateStat\":true,\"welltypes\":[\"LC\",\"SAMPLE\",\"SAMPLE\",\"HC\"],\"highWelltype\":\"HCG\",\"lowWelltype\":\"LC\",\"featureValues\":[1.0,2.0,3.0,5.0]}",
                "CalculationService"
        );

        stubGetWells();
        stubNewScriptExecution(input1);
        stubExecute(input1);
        completeInputSuccessfully(input1, "{\"plateValue\": 42, \"welltypeValues\": {\"HC\": 10, \"LC\": 50, \"SAMPLE\": 61}}");

        stubNewScriptExecution(input2);
        stubExecute(input2);
        input2.getOutput().cancel(true); // this will cause the waitForOutput to throw a CancellationException which is catched by the Throwable case

        var success = featureStatExecutor.executeFeatureStat(cctx, feature, createResultData());

        Assertions.assertFalse(success);

        assertPlateFeatureStatResult(0L, "count", 42);
        assertFeatureStatResult("HC", 1L, "count", 10);
        assertFeatureStatResult("LC", 2L, "count", 50);
        assertFeatureStatResult("SAMPLE", 3L, "count", 61);

        Assertions.assertTrue(cctx.errorCollector().hasError());
        Assertions.assertEquals(1, cctx.errorCollector().getErrors().size());
        Assertions.assertEquals("executing featureStat => waiting for output to be received => exception during execution", cctx.errorCollector().getErrors().get(0).getDescription());
        Assertions.assertNull(cctx.errorCollector().getErrors().get(0).getExceptionMessage());
        Assertions.assertEquals("CancellationException", cctx.errorCollector().getErrors().get(0).getExceptionClassName());
        Assertions.assertEquals(1L, cctx.errorCollector().getErrors().get(0).getFeatureId());
        Assertions.assertEquals(14L, cctx.errorCollector().getErrors().get(0).getFormulaId());
        Assertions.assertEquals("min", cctx.errorCollector().getErrors().get(0).getFormulaName());

        verifyNoMoreInteractions(plateServiceClient, scriptEngineClient);
    }

    @Test
    public void threadInterruptedWhileWaitingForOutputTest() throws Exception {
        var plate = PlateDTO.builder().id(10L).build();

        var formula1 = createFormula("JavaStat::count", "count", 13L);
        var featureStat1 = createFeatureStat(formula1);

        // featureStat2 will cause an exception
        var formula2 = createFormula("JavaStat::min", "min", 14L);
        var featureStat2 = createFeatureStat(formula2);

        var feature = new Feature(1L, "Feature1", null, null, "AFormat", FeatureType.CALCULATION, 0,
                new Formula(1L, "abc_duplicator", null, Category.CALCULATION, "output <- input$abc * 2", ScriptLanguage.R, CalculationScope.WELL, "me", LocalDateTime.now(), "me", LocalDateTime.now()),
                List.of(new CalculationInputValue(1L, 1L, "abc", null, "abc")), List.of(featureStat1, featureStat2));

        var protocol = new Protocol(1L, "TestProtocol", null, true, true, "LC", "HCG",
                new HashMap<>() {{
                    put(0, new Sequence(0, List.of(feature)));
                }});
        var cctx = new CalculationContext(plate, protocol, 1L, 2L, new ErrorCollector(), new ConcurrentHashMap<>());

        var input1 = new ScriptExecution(new TargetRuntime("JAVASTAT", "fast-lane", "v1"), formula1.getFormula(),
                "{\"isWelltypeStat\":true,\"isPlateStat\":true,\"welltypes\":[\"LC\",\"SAMPLE\",\"SAMPLE\",\"HC\"],\"highWelltype\":\"HCG\",\"lowWelltype\":\"LC\",\"featureValues\":[1.0,2.0,3.0,5.0]}",
                "CalculationService"
        );

        var input2 = new ScriptExecution(new TargetRuntime("JAVASTAT", "fast-lane", "v1"), formula2.getFormula(),
                "{\"isWelltypeStat\":true,\"isPlateStat\":true,\"welltypes\":[\"LC\",\"SAMPLE\",\"SAMPLE\",\"HC\"],\"highWelltype\":\"HCG\",\"lowWelltype\":\"LC\",\"featureValues\":[1.0,2.0,3.0,5.0]}",
                "CalculationService"
        );

        stubGetWells();
        stubNewScriptExecution(input1);
        stubExecute(input1);
        completeInputSuccessfully(input1, "{\"plateValue\": 42, \"welltypeValues\": {\"HC\": 10, \"LC\": 50, \"SAMPLE\": 61}}");

        stubNewScriptExecution(input2);
        stubExecute(input2);

        var mainThread = Thread.currentThread();
        new Timer().schedule(new TimerTask() {
            @Override
            public void run() {
                mainThread.interrupt();
            }
        }, 2000);

        var success = featureStatExecutor.executeFeatureStat(cctx, feature, createResultData());

        Assertions.assertFalse(success);

        assertPlateFeatureStatResult(0L, "count", 42);
        assertFeatureStatResult("HC", 1L, "count", 10);
        assertFeatureStatResult("LC", 2L, "count", 50);
        assertFeatureStatResult("SAMPLE", 3L, "count", 61);

        Assertions.assertTrue(cctx.errorCollector().hasError());
        Assertions.assertEquals(1, cctx.errorCollector().getErrors().size());
        Assertions.assertEquals("executing featureStat => waiting for output to be received => interrupted", cctx.errorCollector().getErrors().get(0).getDescription());
        Assertions.assertNull(cctx.errorCollector().getErrors().get(0).getExceptionMessage());
        Assertions.assertEquals("InterruptedException", cctx.errorCollector().getErrors().get(0).getExceptionClassName());
        Assertions.assertEquals(1L, cctx.errorCollector().getErrors().get(0).getFeatureId());
        Assertions.assertEquals(14L, cctx.errorCollector().getErrors().get(0).getFormulaId());
        Assertions.assertEquals("min", cctx.errorCollector().getErrors().get(0).getFormulaName());

        verifyNoMoreInteractions(plateServiceClient, scriptEngineClient);
    }

    @Test
    public void unParsableOutputTest() throws Exception {
        var plate = PlateDTO.builder().id(10L).build();
        var formula = createFormula("JavaStat::count", "count", 13L);
        var featureStat = createFeatureStat(formula);

        var feature = new Feature(1L, "Feature1", null, null, "AFormat", FeatureType.CALCULATION, 0,
                new Formula(1L, "abc_duplicator", null, Category.CALCULATION, "output <- input$abc * 2", ScriptLanguage.R, CalculationScope.WELL, "me", LocalDateTime.now(), "me", LocalDateTime.now()),
                List.of(new CalculationInputValue(1L, 1L, "abc", null, "abc")), List.of(featureStat));

        var protocol = new Protocol(1L, "TestProtocol", null, true, true, "LC", "HCG",
                new HashMap<>() {{
                    put(0, new Sequence(0, List.of(feature)));
                }});
        var cctx = new CalculationContext(plate, protocol, 1L, 2L, new ErrorCollector(), new ConcurrentHashMap<>());

        var input = new ScriptExecution(new TargetRuntime("JAVASTAT", "fast-lane", "v1"), formula.getFormula(),
                "{\"isWelltypeStat\":true,\"isPlateStat\":true,\"welltypes\":[\"LC\",\"SAMPLE\",\"SAMPLE\",\"HC\"],\"highWelltype\":\"HCG\",\"lowWelltype\":\"LC\",\"featureValues\":[1.0,2.0,3.0,5.0]}",
                "CalculationService"
        );

        stubGetWells();
        stubNewScriptExecution(input);
        stubExecute(input);
        completeInputSuccessfully(input, "{\"plateValue\": 42, \"welltypeValues\": {\"HC\": 10, \"LC\": 43, \"SAMPLE\": 52}");

        var success = featureStatExecutor.executeFeatureStat(cctx, feature, createResultData());

        Assertions.assertFalse(success);
        Assertions.assertTrue(cctx.errorCollector().hasError());
        Assertions.assertEquals(1, cctx.errorCollector().getErrors().size());
        Assertions.assertEquals("executing featureStat => processing output => parsing output", cctx.errorCollector().getErrors().get(0).getDescription());
        Assertions.assertNotNull(cctx.errorCollector().getErrors().get(0).getExceptionMessage());
        Assertions.assertEquals("JsonEOFException", cctx.errorCollector().getErrors().get(0).getExceptionClassName());
        Assertions.assertEquals(1L, cctx.errorCollector().getErrors().get(0).getFeatureId());
        Assertions.assertEquals(13L, cctx.errorCollector().getErrors().get(0).getFormulaId());
        Assertions.assertEquals("count", cctx.errorCollector().getErrors().get(0).getFormulaName());

        verifyNoMoreInteractions(plateServiceClient, scriptEngineClient);
    }

    @Test
    public void errorWhileSavingResultDataTest() throws Exception {
        var mockResultDataServiceClient = mockUnimplemented(ResultDataServiceClient.class);
        var featureStatExecutor = new FeatureStatExecutor(plateServiceClient, scriptEngineClient, objectMapper, mockResultDataServiceClient, modelMapper);
        var plate = PlateDTO.builder().id(10L).build();
        var formula = createFormula("JavaStat::count", "count", 13L);
        var featureStat = createFeatureStat(formula, false);

        var feature = new Feature(1L, "Feature1", null, null, "AFormat", FeatureType.CALCULATION, 0,
                new Formula(1L, "abc_duplicator", null, Category.CALCULATION, "output <- input$abc * 2", ScriptLanguage.R, CalculationScope.WELL, "me", LocalDateTime.now(), "me", LocalDateTime.now()),
                List.of(new CalculationInputValue(1L, 1L, "abc", null, "abc")), List.of(featureStat));

        var protocol = new Protocol(1L, "TestProtocol", null, true, true, "LC", "HCG",
                new HashMap<>() {{
                    put(0, new Sequence(0, List.of(feature)));
                }});
        var cctx = new CalculationContext(plate, protocol, 1L, 2L, new ErrorCollector(), new ConcurrentHashMap<>());

        var input = new ScriptExecution(new TargetRuntime("JAVASTAT", "fast-lane", "v1"), formula.getFormula(),
                "{\"isWelltypeStat\":false,\"isPlateStat\":true,\"welltypes\":[\"LC\",\"SAMPLE\",\"SAMPLE\",\"HC\"],\"highWelltype\":\"HCG\",\"lowWelltype\":\"LC\",\"featureValues\":[1.0,2.0,3.0,5.0]}",
                "CalculationService"
        );

        stubGetWells();
        stubNewScriptExecution(input);
        stubExecute(input);
        completeInputSuccessfully(input, "{\"plateValue\": 42, \"welltypeValues\": {}}");

        doThrow(new ResultFeatureStatUnresolvableException("Error while creating ResultFeatureStat")).when(mockResultDataServiceClient).createResultFeatureStat(1L, 1L, 13L, 42, "count", null, StatusCode.SUCCESS, "Ok", 0);

        var success = featureStatExecutor.executeFeatureStat(cctx, feature, createResultData());

        Assertions.assertFalse(success);
        Assertions.assertTrue(cctx.errorCollector().hasError());
        Assertions.assertEquals(1, cctx.errorCollector().getErrors().size());
        Assertions.assertEquals("executing featureStat  => processing output => saving resultdata", cctx.errorCollector().getErrors().get(0).getDescription());
        Assertions.assertEquals("Error while creating ResultFeatureStat", cctx.errorCollector().getErrors().get(0).getExceptionMessage());
        Assertions.assertEquals("ResultFeatureStatUnresolvableException", cctx.errorCollector().getErrors().get(0).getExceptionClassName());
        Assertions.assertEquals(1L, cctx.errorCollector().getErrors().get(0).getFeatureId());
        Assertions.assertEquals(13L, cctx.errorCollector().getErrors().get(0).getFormulaId());
        Assertions.assertEquals("count", cctx.errorCollector().getErrors().get(0).getFormulaName());

        verifyNoMoreInteractions(plateServiceClient, scriptEngineClient);
    }

    @Test
    public void scriptIndicatesBadRequestException() throws Exception {
        var plate = PlateDTO.builder().id(10L).build();

        var formula1 = createFormula("JavaStat::count", "count", 13L);
        var featureStat1 = createFeatureStat(formula1);

        var formula2 = createFormula("JavaStat::min", "min", 14L);
        var featureStat2 = createFeatureStat(formula2);

        var feature = new Feature(1L, "Feature1", null, null, "AFormat", FeatureType.CALCULATION, 0,
                new Formula(1L, "abc_duplicator", null, Category.CALCULATION, "output <- input$abc * 2", ScriptLanguage.R, CalculationScope.WELL, "me", LocalDateTime.now(), "me", LocalDateTime.now()),
                List.of(new CalculationInputValue(1L, 1L, "abc", null, "abc")), List.of(featureStat1, featureStat2));

        var protocol = new Protocol(1L, "TestProtocol", null, true, true, "LC", "HCG",
                new HashMap<>() {{
                    put(0, new Sequence(0, List.of(feature)));
                }});
        var cctx = new CalculationContext(plate, protocol, 1L, 2L, new ErrorCollector(), new ConcurrentHashMap<>());

        var input1 = new ScriptExecution(new TargetRuntime("JAVASTAT", "fast-lane", "v1"), formula1.getFormula(),
                "{\"isWelltypeStat\":true,\"isPlateStat\":true,\"welltypes\":[\"LC\",\"SAMPLE\",\"SAMPLE\",\"HC\"],\"highWelltype\":\"HCG\",\"lowWelltype\":\"LC\",\"featureValues\":[1.0,2.0,3.0,5.0]}",
                "CalculationService"
        );

        var input2 = new ScriptExecution(new TargetRuntime("JAVASTAT", "fast-lane", "v1"), formula2.getFormula(),
                "{\"isWelltypeStat\":true,\"isPlateStat\":true,\"welltypes\":[\"LC\",\"SAMPLE\",\"SAMPLE\",\"HC\"],\"highWelltype\":\"HCG\",\"lowWelltype\":\"LC\",\"featureValues\":[1.0,2.0,3.0,5.0]}",
                "CalculationService"
        );

        stubGetWells();
        stubNewScriptExecution(input1);
        stubExecute(input1);
        completeInputSuccessfully(input1, "{\"plateValue\": 42, \"welltypeValues\": {\"HC\": 10, \"LC\": 50, \"SAMPLE\": 61}}");

        stubNewScriptExecution(input2);
        stubExecute(input2);
        input2.getOutput().complete(new ScriptExecutionOutputDTO(input2.getScriptExecutionInput().getId(), "Example error message", ResponseStatusCode.BAD_REQUEST, "Ok", 0));

        var success = featureStatExecutor.executeFeatureStat(cctx, feature, createResultData());

        Assertions.assertFalse(success);
        Assertions.assertTrue(cctx.errorCollector().hasError());
        Assertions.assertEquals(1, cctx.errorCollector().getErrors().size());
        Assertions.assertEquals("executing featureStat => processing output => output indicates bad request", cctx.errorCollector().getErrors().get(0).getDescription());
        Assertions.assertNull(cctx.errorCollector().getErrors().get(0).getExceptionMessage());
        Assertions.assertNull(cctx.errorCollector().getErrors().get(0).getExceptionClassName());
        Assertions.assertEquals(1L, cctx.errorCollector().getErrors().get(0).getFeatureId());
        Assertions.assertEquals(14L, cctx.errorCollector().getErrors().get(0).getFeatureStatId());
        Assertions.assertEquals(14L, cctx.errorCollector().getErrors().get(0).getFormulaId());
        Assertions.assertEquals("min", cctx.errorCollector().getErrors().get(0).getFormulaName());

        // results of first featureStat should still be saved
        assertPlateFeatureStatResult(0L, "count", 42);
        assertFeatureStatResult("HC", 1L, "count", 10);
        assertFeatureStatResult("LC", 2L, "count", 50);
        assertFeatureStatResult("SAMPLE", 3L, "count", 61);

        verifyNoMoreInteractions(plateServiceClient, scriptEngineClient);
    }

    @Test
    public void scriptIndicatesScriptErrorException() throws Exception {
        var plate = PlateDTO.builder().id(10L).build();

        var formula1 = createFormula("JavaStat::count", "count", 13L);
        var featureStat1 = createFeatureStat(formula1);

        var formula2 = createFormula("JavaStat::min", "min", 14L);
        var featureStat2 = createFeatureStat(formula2);

        var feature = new Feature(1L, "Feature1", null, null, "AFormat", FeatureType.CALCULATION, 0,
                new Formula(1L, "abc_duplicator", null, Category.CALCULATION, "output <- input$abc * 2", ScriptLanguage.R, CalculationScope.WELL, "me", LocalDateTime.now(), "me", LocalDateTime.now()),
                List.of(new CalculationInputValue(1L, 1L, "abc", null, "abc")), List.of(featureStat1, featureStat2));

        var protocol = new Protocol(1L, "TestProtocol", null, true, true, "LC", "HCG",
                new HashMap<>() {{
                    put(0, new Sequence(0, List.of(feature)));
                }});
        var cctx = new CalculationContext(plate, protocol, 1L, 2L, new ErrorCollector(), new ConcurrentHashMap<>());

        var input1 = new ScriptExecution(new TargetRuntime("JAVASTAT", "fast-lane", "v1"), formula1.getFormula(),
                "{\"isWelltypeStat\":true,\"isPlateStat\":true,\"welltypes\":[\"LC\",\"SAMPLE\",\"SAMPLE\",\"HC\"],\"highWelltype\":\"HCG\",\"lowWelltype\":\"LC\",\"featureValues\":[1.0,2.0,3.0,5.0]}",
                "CalculationService"
        );

        var input2 = new ScriptExecution(new TargetRuntime("JAVASTAT", "fast-lane", "v1"), formula2.getFormula(),
                "{\"isWelltypeStat\":true,\"isPlateStat\":true,\"welltypes\":[\"LC\",\"SAMPLE\",\"SAMPLE\",\"HC\"],\"highWelltype\":\"HCG\",\"lowWelltype\":\"LC\",\"featureValues\":[1.0,2.0,3.0,5.0]}",
                "CalculationService"
        );

        stubGetWells();
        stubNewScriptExecution(input1);
        stubExecute(input1);
        completeInputSuccessfully(input1, "{\"plateValue\": 42, \"welltypeValues\": {\"HC\": 10, \"LC\": 50, \"SAMPLE\": 61}}");

        stubNewScriptExecution(input2);
        stubExecute(input2);
        input2.getOutput().complete(new ScriptExecutionOutputDTO(input2.getScriptExecutionInput().getId(), "Example error message", ResponseStatusCode.SCRIPT_ERROR, "Ok", 0));

        var success = featureStatExecutor.executeFeatureStat(cctx, feature, createResultData());

        Assertions.assertFalse(success);
        Assertions.assertTrue(cctx.errorCollector().hasError());
        Assertions.assertEquals(1, cctx.errorCollector().getErrors().size());
        Assertions.assertEquals("executing featureStat => processing output => output indicates script error", cctx.errorCollector().getErrors().get(0).getDescription());
        Assertions.assertNull(cctx.errorCollector().getErrors().get(0).getExceptionMessage());
        Assertions.assertNull(cctx.errorCollector().getErrors().get(0).getExceptionClassName());
        Assertions.assertEquals(1L, cctx.errorCollector().getErrors().get(0).getFeatureId());
        Assertions.assertEquals(14L, cctx.errorCollector().getErrors().get(0).getFeatureStatId());
        Assertions.assertEquals(14L, cctx.errorCollector().getErrors().get(0).getFormulaId());
        Assertions.assertEquals("min", cctx.errorCollector().getErrors().get(0).getFormulaName());

        // results of first featureStat should still be saved
        assertPlateFeatureStatResult(0L, "count", 42);
        assertFeatureStatResult("HC", 1L, "count", 10);
        assertFeatureStatResult("LC", 2L, "count", 50);
        assertFeatureStatResult("SAMPLE", 3L, "count", 61);

        verifyNoMoreInteractions(plateServiceClient, scriptEngineClient);
    }

    @Test
    public void errorWhileFetchingWellInfoTest() throws Exception {
        var plate = PlateDTO.builder().id(10L).build();
        var formula = createFormula("JavaStat::count", "count", 13L);
        var featureStat = createFeatureStat(formula);

        var feature = new Feature(1L, "Feature1", null, null, "AFormat", FeatureType.CALCULATION, 0,
                new Formula(1L, "abc_duplicator", null, Category.CALCULATION, "output <- input$abc * 2", ScriptLanguage.R, CalculationScope.WELL, "me", LocalDateTime.now(), "me", LocalDateTime.now()),
                List.of(new CalculationInputValue(1L, 1L, "abc", null, "abc")), List.of(featureStat));

        var protocol = new Protocol(1L, "TestProtocol", null, true, true, "LC", "HCG",
                new HashMap<>() {{
                    put(0, new Sequence(0, List.of(feature)));
                }});
        var cctx = new CalculationContext(plate, protocol, 1L, 2L, new ErrorCollector(), new ConcurrentHashMap<>());

        doThrow(new PlateUnresolvableException("Error while fetching plate")).when(plateServiceClient).getWellsOfPlateSorted(10L);
        var success = featureStatExecutor.executeFeatureStat(cctx, feature, createResultData());

        Assertions.assertFalse(success);
        Assertions.assertTrue(cctx.errorCollector().hasError());
        Assertions.assertEquals(1, cctx.errorCollector().getErrors().size());
        Assertions.assertEquals("executing featureStat => fetching wells => exception", cctx.errorCollector().getErrors().get(0).getDescription());
        Assertions.assertEquals("Error while fetching plate", cctx.errorCollector().getErrors().get(0).getExceptionMessage());
        Assertions.assertEquals("PlateUnresolvableException", cctx.errorCollector().getErrors().get(0).getExceptionClassName());
        Assertions.assertEquals(1L, cctx.errorCollector().getErrors().get(0).getFeatureId());
        Assertions.assertNull(cctx.errorCollector().getErrors().get(0).getFeatureStatId());
        Assertions.assertNull(cctx.errorCollector().getErrors().get(0).getFormulaId());

        verifyNoMoreInteractions(plateServiceClient, scriptEngineClient);
    }

    private void stubNewScriptExecution(ScriptExecution scriptExecution) {
        doReturn(scriptExecution).when(scriptEngineClient).newScriptExecution(JAVASTAT_FAST_LANE, scriptExecution.getScriptExecutionInput().getScript(), scriptExecution.getScriptExecutionInput().getInput());
    }

    private void stubExecute(ScriptExecution input) throws JsonProcessingException {
        doNothing().when(scriptEngineClient).execute(input);
    }

    private void stubGetWells() throws PlateUnresolvableException {
        doReturn(List.of(
                new WellDTO(1L, 10L, 1, 1, "LC", WellStatus.ACCEPTED_DEFAULT, 1L, ""),
                new WellDTO(1L, 10L, 1, 2, "SAMPLE", WellStatus.ACCEPTED_DEFAULT, 1L, ""),
                new WellDTO(1L, 10L, 1, 3, "SAMPLE", WellStatus.ACCEPTED_DEFAULT, 1L, ""),
                new WellDTO(1L, 10L, 1, 3, "HC", WellStatus.ACCEPTED_DEFAULT, 1L, ""))
        ).when(plateServiceClient).getWellsOfPlateSorted(10L);
    }

    private void completeInputSuccessfully(ScriptExecution input, String output) {
        input.getOutput().complete(new ScriptExecutionOutputDTO(input.getScriptExecutionInput().getId(), output, ResponseStatusCode.SUCCESS, "Ok", 0));
    }

    private ResultDataDTO createResultData() {
        return ResultDataDTO.builder()
                .featureId(1L)
                .id(25L)
                .exitCode(0)
                .statusCode(StatusCode.SUCCESS)
                .statusMessage("Ok")
                .resultSetId(25L)
                .values(new float[]{1.0f, 2.0f, 3.0f, 5.0f})
                .createdTimestamp(LocalDateTime.now())
                .build();
    }

    private Formula createFormula(String formula, String name, Long id) {
        return Formula.builder()
                .formula(formula)
                .category(Category.CALCULATION)
                .name(name)
                .id(id)
                .language(ScriptLanguage.JAVASTAT)
                .scope(CalculationScope.PLATE)
                .build();
    }

    private FeatureStat createFeatureStat(Formula formula) {
        return createFeatureStat(formula, true);
    }

    private FeatureStat createFeatureStat(Formula formula, Boolean welltypeStat) {
        return FeatureStat.builder()
                .id(formula.getId())
                .formula(formula)
                .featureId(1L)
                .plateStat(true)
                .welltypeStat(welltypeStat)
                .name(formula.getName())
                .build();
    }

    private void assertPlateFeatureStatResult(Long id, String name, float value) throws ResultFeatureStatUnresolvableException {
        var result = resultDataServiceClient.getResultFeatureStat(1, id);
        Assertions.assertNull(result.getWelltype());
        Assertions.assertEquals(StatusCode.SUCCESS, result.getStatusCode());
        Assertions.assertEquals(name, result.getStatisticName());
        Assertions.assertEquals(value, result.getValue());
    }

    private void assertFeatureStatResult(String welltype, Long id, String name, float value) throws ResultFeatureStatUnresolvableException {
        var result = resultDataServiceClient.getResultFeatureStat(1, id);
        Assertions.assertEquals(welltype, result.getWelltype());
        Assertions.assertEquals(StatusCode.SUCCESS, result.getStatusCode());
        Assertions.assertEquals(name, result.getStatisticName());
        Assertions.assertEquals(value, result.getValue());
    }

}
