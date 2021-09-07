package eu.openanalytics.phaedra.calculationservice.service;

import eu.openanalytics.phaedra.calculationservice.model.CalculationInputValue;
import eu.openanalytics.phaedra.calculationservice.model.Feature;
import eu.openanalytics.phaedra.calculationservice.model.Protocol;
import eu.openanalytics.phaedra.calculationservice.model.Sequence;
import eu.openanalytics.phaedra.protocolservice.client.ProtocolServiceClient;
import eu.openanalytics.phaedra.protocolservice.client.exception.ProtocolUnresolvableException;
import eu.openanalytics.phaedra.protocolservice.dto.FeatureDTO;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Service responsible for collecting all the information regarding a single Protocol which is required for executing
 * the protocol.
 */
@Service
public class ProtocolInfoCollector {

    private final ProtocolServiceClient protocolServiceClient;
    private final ModelMapper modelMapper;
    private final FormulaService formulaService;

    public ProtocolInfoCollector(ProtocolServiceClient protocolServiceClient, ModelMapper modelMapper, FormulaService formulaService) {
        this.protocolServiceClient = protocolServiceClient;
        this.modelMapper = modelMapper;
        this.formulaService = formulaService;
    }

    public Protocol getProtocol(long protocolId) throws ProtocolUnresolvableException {
        // 1. get protocol
        var protocol = modelMapper.map(protocolServiceClient.getProtocol(protocolId));

        // 2. get features
        var features = protocolServiceClient.getFeaturesOfProtocol(protocolId);

        // 3. get formulas corresponding to the features
        var formulaIds = features.stream()
                .map(FeatureDTO::getFormulaId)
                .collect(Collectors.toUnmodifiableList());
        var formulas = formulaService.getFormulasByIds(formulaIds);

        // 4. get CalculationInputValues corresponding to the feature
        var calculationInputValues = protocolServiceClient.getCalculationInputValuesOfProtocol(protocolId);
        // -> convert it to a map for easier lookup
        var calculationInputValuesMap = calculationInputValues
                .stream()
                .map((civ) -> modelMapper.map(civ).build())
                .collect(Collectors.groupingBy(
                        CalculationInputValue::getFeatureId,
                        Collectors.toList()
                ));

        // 5. create features with corresponding formulas and calculationInputValues
        var resFeatures = new HashMap<Integer, List<Feature>>();
        for (var featureDTO : features) {
            resFeatures.putIfAbsent(featureDTO.getSequence(), new ArrayList<>());

            var formula = formulas.get(featureDTO.getFormulaId());
            if (formula == null) {
                throw new ProtocolUnresolvableException(String.format("Did not found formula for feature with id %s", featureDTO.getId()));
            }

            var civs = calculationInputValuesMap.get(featureDTO.getId());
            if (civs == null || civs.isEmpty()) {
                throw new ProtocolUnresolvableException(String.format("Did not found civs for feature with id %s", featureDTO.getId()));
            }

            var feature = modelMapper.map(featureDTO)
                    .formula(formula)
                    .calculationInputValues(civs)
                    .build();

            resFeatures.get(feature.getSequence()).add(feature);
        }

        var resSequences = new HashMap<Integer, Sequence>();
        for (var entry : resFeatures.entrySet()) {
            resSequences.put(entry.getKey(), new Sequence(entry.getKey(), entry.getValue()));
        }

        // 5. create protocol
        return protocol.sequences(resSequences).build();
    }
}
