package eu.openanalytics.phaedra.calculationservice.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.openanalytics.phaedra.calculationservice.controller.clients.MeasServiceClient;
import eu.openanalytics.phaedra.calculationservice.controller.clients.MeasUnresolvableException;
import eu.openanalytics.phaedra.calculationservice.controller.clients.PlateServiceClient;
import eu.openanalytics.phaedra.calculationservice.controller.clients.ProtocolServiceClient;
import eu.openanalytics.phaedra.calculationservice.controller.clients.ProtocolUnresolvableException;
import eu.openanalytics.phaedra.calculationservice.enumeration.Category;
import eu.openanalytics.phaedra.calculationservice.enumeration.ScriptLanguage;
import eu.openanalytics.phaedra.calculationservice.model.Feature;
import eu.openanalytics.phaedra.calculationservice.model.Protocol;
import eu.openanalytics.phaedra.calculationservice.model.Sequence;
import eu.openanalytics.phaedra.calculationservice.scriptengineclient.client.ScriptEngineClient;
import eu.openanalytics.phaedra.calculationservice.scriptengineclient.model.ScriptExecutionInput;
import eu.openanalytics.phaedra.calculationservice.scriptengineclient.model.ScriptExecutionOutput;
import org.apache.commons.lang.NotImplementedException;
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

    public ProtocolExecutorService(ProtocolServiceClient protocolServiceClient, PlateServiceClient plateServiceClient, MeasServiceClient measServiceClient, ScriptEngineClient scriptEngineClient) {
        this.protocolServiceClient = protocolServiceClient;
        this.plateServiceClient = plateServiceClient;
        this.measServiceClient = measServiceClient;
        this.scriptEngineClient = scriptEngineClient;
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

            // sequentially execute every sequence
            for (var seq = 0; seq < protocol.getSequences().size(); seq++) {
                Sequence currentSequence = protocol.getSequences().get(seq);

                // 1. asynchronously create inputs and submit them to the ScriptEngine
                var inputs = new ArrayList<Future<ScriptExecutionInput>>();
                for (var feature : currentSequence.getFeatures()) {
                    inputs.add(executorService.submit(() -> {
                        try {
                            return executeFeature(feature, measId);
                        } catch (MeasUnresolvableException | JsonProcessingException | ExecutionException | InterruptedException e) {
                            e.printStackTrace();
                            return null; // TODO
                        }
                    }));
                }

                // 2. step convert input into outputs by waiting (and blocking!) for the futures to complete
                var outputs = new ArrayList<ScriptExecutionOutput>();
                for (var future : inputs) {
                    outputs.add(future.get().getOutput().get());
                }

                // 3. process output
                System.out.println(outputs);

                // 4. process next sequences
            }

            // 5. set plate status

        } catch (ProtocolUnresolvableException | ExecutionException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    private ScriptExecutionInput executeFeature(Feature feature, long measId) throws MeasUnresolvableException, JsonProcessingException, ExecutionException, InterruptedException {
        var inputVariables = collectVariablesForFeature(feature, measId);
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

    private HashMap<String, float[]> collectVariablesForFeature(Feature feature, long measId) throws MeasUnresolvableException {
        var inputVariables = new HashMap<String, float[]>();

        for (var civ : feature.getCalculationInputValues()) {
            if (inputVariables.containsKey(civ.getVariableName())) {
                throw new RuntimeException("Double variable name!");
            }

            if (civ.getSourceFeatureId() != null) {
                throw new NotImplementedException("CIV with source feature id is currently not supporrted");
            } else if (civ.getSourceMeasColName() != null) {
                inputVariables.put(civ.getVariableName(), measServiceClient.getWellData(measId, civ.getSourceMeasColName()));
            } else {
                throw new IllegalStateException("should not happen.."); // TODO
            }
        }

        return inputVariables;
    }



}
