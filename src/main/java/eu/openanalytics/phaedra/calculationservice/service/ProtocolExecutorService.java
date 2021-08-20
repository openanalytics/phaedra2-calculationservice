package eu.openanalytics.phaedra.calculationservice.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.openanalytics.phaedra.calculationservice.controller.clients.MeasServiceClient;
import eu.openanalytics.phaedra.calculationservice.controller.clients.MeasUnresolvableException;
import eu.openanalytics.phaedra.calculationservice.controller.clients.PlateServiceClient;
import eu.openanalytics.phaedra.calculationservice.controller.clients.PlateUnresolvableException;
import eu.openanalytics.phaedra.calculationservice.controller.clients.ProtocolServiceClient;
import eu.openanalytics.phaedra.calculationservice.controller.clients.ProtocolUnresolvableException;
import eu.openanalytics.phaedra.calculationservice.enumeration.Category;
import eu.openanalytics.phaedra.calculationservice.enumeration.ScriptLanguage;
import eu.openanalytics.phaedra.calculationservice.model.Feature;
import eu.openanalytics.phaedra.calculationservice.model.Plate;
import eu.openanalytics.phaedra.calculationservice.model.Protocol;
import eu.openanalytics.phaedra.calculationservice.model.Sequence;
import eu.openanalytics.phaedra.calculationservice.scriptengineclient.client.ScriptEngineClient;
import org.apache.commons.lang.NotImplementedException;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.concurrent.ExecutionException;

import static eu.openanalytics.phaedra.calculationservice.CalculationService.R_FAST_LANE;

@Service
public class ProtocolExecutorService {

    private final ProtocolServiceClient protocolServiceClient;
    private final PlateServiceClient plateServiceClient;
    private final MeasServiceClient measServiceClient;
    private final ScriptEngineClient scriptEngineClient;
    private final ObjectMapper objectMapper;

    public ProtocolExecutorService(ProtocolServiceClient protocolServiceClient, PlateServiceClient plateServiceClient, MeasServiceClient measServiceClient, ScriptEngineClient scriptEngineClient) {
        this.protocolServiceClient = protocolServiceClient;
        this.plateServiceClient = plateServiceClient;
        this.measServiceClient = measServiceClient;
        this.scriptEngineClient = scriptEngineClient;
        this.objectMapper = new ObjectMapper(); // TODO thread-safe?
    }

    public void execute(long protocolId, long plateId, long measId) throws ProtocolUnresolvableException, PlateUnresolvableException, MeasUnresolvableException, JsonProcessingException, ExecutionException, InterruptedException {
        Protocol protocol = protocolServiceClient.getProtocol(protocolId);
        Plate plate = plateServiceClient.getPlate(plateId);

        Sequence currentSequence = protocol.getSequences().get(0); // TODO start at 0 or 1

        for (var feature : currentSequence.getFeatures()) {
            executeFeature(protocol, plate, feature, measId);
        }

    }

    private void executeFeature(Protocol protocol, Plate plate, Feature feature, long measId) throws MeasUnresolvableException, JsonProcessingException, ExecutionException, InterruptedException {
        var inputVariables = new HashMap<String, float[]>();
        System.out.println(feature.getCalculationInputValues());
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
        if (feature.getFormula().getCategory() != Category.CALCULATION
                || feature.getFormula().getLanguage() != ScriptLanguage.R) {
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

        var output = execution.getOutput().get();
        System.out.println(output);

    }


}
