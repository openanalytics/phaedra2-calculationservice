package eu.openanalytics.phaedra.calculationservice.service;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.openanalytics.phaedra.calculationservice.controller.clients.MeasServiceClient;
import eu.openanalytics.phaedra.calculationservice.controller.clients.MeasUnresolvableException;
import eu.openanalytics.phaedra.calculationservice.controller.clients.PlateServiceClient;
import eu.openanalytics.phaedra.calculationservice.controller.clients.ProtocolServiceClient;
import eu.openanalytics.phaedra.calculationservice.controller.clients.ProtocolUnresolvableException;
import eu.openanalytics.phaedra.calculationservice.controller.clients.ResultDataServiceClient;
import eu.openanalytics.phaedra.calculationservice.controller.clients.ResultDataUnresolvableException;
import eu.openanalytics.phaedra.calculationservice.controller.clients.ResultSetUnresolvableException;
import eu.openanalytics.phaedra.calculationservice.enumeration.Category;
import eu.openanalytics.phaedra.calculationservice.enumeration.ScriptLanguage;
import eu.openanalytics.phaedra.calculationservice.model.Feature;
import eu.openanalytics.phaedra.calculationservice.model.Protocol;
import eu.openanalytics.phaedra.calculationservice.model.Sequence;
import eu.openanalytics.phaedra.calculationservice.scriptengineclient.client.ScriptEngineClient;
import eu.openanalytics.phaedra.calculationservice.scriptengineclient.model.ResponseStatusCode;
import eu.openanalytics.phaedra.calculationservice.scriptengineclient.model.ScriptExecutionInput;
import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static eu.openanalytics.phaedra.calculationservice.CalculationService.R_FAST_LANE;

@Service
public class ProtocolExecutorService {

    private final ProtocolServiceClient protocolServiceClient;
    private final PlateServiceClient plateServiceClient;
    private final MeasServiceClient measServiceClient;
    private final ScriptEngineClient scriptEngineClient;
    private final ObjectMapper objectMapper;
    private final ThreadPoolExecutor executorService;
    private final ResultDataServiceClient resultDataServiceClient;

    public ProtocolExecutorService(ProtocolServiceClient protocolServiceClient, PlateServiceClient plateServiceClient, MeasServiceClient measServiceClient, ScriptEngineClient scriptEngineClient, ResultDataServiceClient resultDataServiceClient) {
        this.protocolServiceClient = protocolServiceClient;
        this.plateServiceClient = plateServiceClient;
        this.measServiceClient = measServiceClient;
        this.scriptEngineClient = scriptEngineClient;
        this.resultDataServiceClient = resultDataServiceClient;
        this.objectMapper = new ObjectMapper(); // TODO thread-safe?

        executorService = new ThreadPoolExecutor(8, 1024, 60L, TimeUnit.SECONDS, new SynchronousQueue<>());
        new Timer().schedule(new TimerTask() {
            @Override
            public void run() {
                System.out.println("Number of active threads " + executorService.getActiveCount());
                System.out.println("Number of threads " + executorService.getPoolSize());
            }
        }, 0, 250);
    }

    public Future<?> execute(long protocolId, long plateId, long measId) {
        // submit execution to the ThreadPool/ExecutorService and return a future
        return executorService.submit(() -> {
            executeProtocol(protocolId, plateId, measId);
            return true;
        });
    }

    private void executeProtocol(long protocolId, long plateId, long measId) {
        try {
            Protocol protocol = protocolServiceClient.getProtocol(protocolId);

            // 0. create ResultDataSet
            var resultSet = resultDataServiceClient.createResultDataSet(protocolId, plateId, measId);

            // sequentially execute every sequence
            for (var seq = 0; seq < protocol.getSequences().size(); seq++) {
                Sequence currentSequence = protocol.getSequences().get(seq);

                // 1. asynchronously create inputs and submit them to the ScriptEngine
                var calculations = new ArrayList<Pair<Feature, Future<ScriptExecutionInput>>>();

                for (var feature : currentSequence.getFeatures()) {
                    calculations.add(Pair.of(feature, executorService.submit(() -> {
                        try {
                            return executeFeature(feature, measId, currentSequence.getSequenceNumber(), resultSet.getId());
                        } catch (MeasUnresolvableException | JsonProcessingException | ExecutionException | InterruptedException e) {
                            e.printStackTrace();
                            return null; // TODO
                        }
                    })));
                }

                // 2. wait (block !) for execution to be sent to the ScriptEngine
                for (var calculation : calculations) {
                    calculation.getRight().get();
                }

                // 3. wait (block!) for the output to be received from the ScriptEngine
                for (var calculation : calculations) {
                    calculation.getRight().get().getOutput().get();
                }

                // 4. process the output
                for (var calculation : calculations) {
                    var feature = calculation.getLeft();
                    var output = calculation.getRight().get().getOutput().get();
                    if (output.getStatusCode() == ResponseStatusCode.SUCCESS) {
                        OutputWrapper outputValue = objectMapper.readValue(output.getOutput(), OutputWrapper.class);
                        resultDataServiceClient.addResultData(resultSet.getId(), feature.getId(), outputValue.output, output.getStatusCode(), output.getStatusMessage(), output.getExitCode());
                    } else if (output.getStatusCode() == ResponseStatusCode.SCRIPT_ERROR) {
                        // TODO
                    } else if (output.getStatusCode() == ResponseStatusCode.WORKER_INTERNAL_ERROR) {
                        // TODO
                    }
                }
                // 4. process next sequences
            }

            // 5. set plate status
            resultDataServiceClient.completeResultDataSet(resultSet.getId(), "Completed");

        } catch (ProtocolUnresolvableException | ExecutionException | InterruptedException | JsonProcessingException | ResultSetUnresolvableException | ResultDataUnresolvableException e) {
            e.printStackTrace();
        }

    }

    private ScriptExecutionInput executeFeature(Feature feature, long measId, long currentSequence, long resultId) throws MeasUnresolvableException, JsonProcessingException, ExecutionException, InterruptedException, ResultDataUnresolvableException {
        var inputVariables = collectVariablesForFeature(feature, measId, currentSequence, resultId);
        if (feature.getFormula().getCategory() != Category.CALCULATION || feature.getFormula().getLanguage() != ScriptLanguage.R) {
//            || feature.getFormula().getScope() != CalculationScope.WELL) { // TODO
            throw new NotImplementedException("This formula is not supported!");
        }

        var script = feature.getFormula().getFormula();

        var execution = scriptEngineClient.newScriptExecution(
                R_FAST_LANE,
                script,
                objectMapper.writeValueAsString(inputVariables)
        );

        scriptEngineClient.execute(execution);

        return execution;
    }

    private HashMap<String, float[]> collectVariablesForFeature(Feature feature, long measId, long currentSequence, long resultId) throws MeasUnresolvableException, ResultDataUnresolvableException {
        var inputVariables = new HashMap<String, float[]>();

        for (var civ : feature.getCalculationInputValues()) {
            if (inputVariables.containsKey(civ.getVariableName())) {
                throw new RuntimeException("Double variable name!");
            }

            if (civ.getSourceFeatureId() != null) {
                if (currentSequence == 0) {
                    throw new IllegalStateException("should not happen.."); // TODO
                }
                inputVariables.put(civ.getVariableName(), resultDataServiceClient.getResultData(resultId, civ.getSourceFeatureId()).getValues());
            } else if (civ.getSourceMeasColName() != null) {
                inputVariables.put(civ.getVariableName(), measServiceClient.getWellData(measId, civ.getSourceMeasColName()));
            } else {
                throw new IllegalStateException("should not happen.."); // TODO
            }
        }

        return inputVariables;
    }

    private static class OutputWrapper {

        public final float[] output;

        @JsonCreator
        private OutputWrapper(@JsonProperty(value = "output", required = true) float[] output) {
            this.output = output;
        }
    }


}
