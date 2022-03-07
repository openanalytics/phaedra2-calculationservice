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
package eu.openanalytics.phaedra.calculationservice.service.protocol;

import eu.openanalytics.phaedra.calculationservice.model.CalculationInputValue;
import eu.openanalytics.phaedra.calculationservice.model.Feature;
import eu.openanalytics.phaedra.calculationservice.model.FeatureStat;
import eu.openanalytics.phaedra.calculationservice.model.Protocol;
import eu.openanalytics.phaedra.calculationservice.model.Sequence;
import eu.openanalytics.phaedra.calculationservice.service.FormulaService;
import eu.openanalytics.phaedra.calculationservice.service.ModelMapper;
import eu.openanalytics.phaedra.protocolservice.client.ProtocolServiceClient;
import eu.openanalytics.phaedra.protocolservice.client.exception.ProtocolUnresolvableException;
import eu.openanalytics.phaedra.protocolservice.dto.FeatureDTO;
import eu.openanalytics.phaedra.protocolservice.dto.FeatureStatDTO;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collections;
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

    public Protocol getProtocol(long protocolId, String... authToken) throws ProtocolUnresolvableException {
        // 1. get protocol
        var protocol = modelMapper.map(protocolServiceClient.getProtocol(protocolId, authToken));

        // 2. get features
        var features = protocolServiceClient.getFeaturesOfProtocol(protocolId, authToken);

        // 3. get CalculationInputValues corresponding to the feature
        var calculationInputValues = protocolServiceClient.getCalculationInputValuesOfProtocol(protocolId, authToken);
        // -> convert it to a map for easier lookup
        var calculationInputValuesMap = calculationInputValues.stream()
                .map((civ) -> modelMapper.map(civ).build())
                .collect(Collectors.groupingBy(
                        CalculationInputValue::getFeatureId,
                        Collectors.toList()
                ));

        // 4. get FeatureStats of protocol
        var featureStats = protocolServiceClient.getFeatureStatsOfProtocol(protocolId, authToken);

        // 5. get formulas corresponding to the features and FeatureStats
        var formulaIds = new ArrayList<>(features.stream()
                .map(FeatureDTO::getFormulaId).toList());
        formulaIds.addAll(featureStats.stream()
                .map(FeatureStatDTO::getFormulaId)
                .toList());
        var formulas = formulaService.getFormulasByIds(formulaIds);

        //  6. convert featureStats to a map for easier lookup + attach formula
        var featureStatsMap = featureStats.stream()
                .map((fs) -> modelMapper.map(fs)
                        .formula(formulas.get(fs.getFormulaId()))
                        .build()
                )
                .collect(Collectors.groupingBy(
                        FeatureStat::getFeatureId,
                        Collectors.toList()
                ));

        // 5. create features with corresponding formulas, calculationInputValues and FeatureStats
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

            var curFeatureStats = featureStatsMap.get(featureDTO.getId());
            if (curFeatureStats == null) {
                curFeatureStats = Collections.emptyList();
            }

            var feature = modelMapper.map(featureDTO)
                    .formula(formula)
                    .calculationInputValues(civs)
                    .featureStats(curFeatureStats)
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

