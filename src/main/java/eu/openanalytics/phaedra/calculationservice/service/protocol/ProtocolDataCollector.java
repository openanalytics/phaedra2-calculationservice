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
package eu.openanalytics.phaedra.calculationservice.service.protocol;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.springframework.stereotype.Service;

import eu.openanalytics.phaedra.calculationservice.model.Formula;
import eu.openanalytics.phaedra.calculationservice.service.FormulaService;
import eu.openanalytics.phaedra.protocolservice.client.ProtocolServiceClient;
import eu.openanalytics.phaedra.protocolservice.client.exception.ProtocolUnresolvableException;
import eu.openanalytics.phaedra.protocolservice.dto.FeatureDTO;
import eu.openanalytics.phaedra.protocolservice.dto.FeatureStatDTO;
import eu.openanalytics.phaedra.protocolservice.dto.ProtocolDTO;

/**
 * Service responsible for collecting all the information regarding
 * a protocol which is required for executing the protocol.
 */
@Service
public class ProtocolDataCollector {

    private final ProtocolServiceClient protocolServiceClient;
    private final FormulaService formulaService;

    public ProtocolDataCollector(ProtocolServiceClient protocolServiceClient, FormulaService formulaService) {
        this.protocolServiceClient = protocolServiceClient;
        this.formulaService = formulaService;
    }

    public ProtocolData getProtocolData(long protocolId) throws ProtocolUnresolvableException {
    	
    	ProtocolData data = new ProtocolData();
    	data.protocol = protocolServiceClient.getProtocol(protocolId);
    
        var featureStats = protocolServiceClient.getFeatureStatsOfProtocol(protocolId);
        data.featureStats = featureStats.stream().collect(Collectors.groupingBy(FeatureStatDTO::getFeatureId));

        // Get formulas corresponding to the features and FeatureStats
        var formulaIds = new ArrayList<>(data.protocol.getFeatures().stream().map(FeatureDTO::getFormulaId).toList());
        formulaIds.addAll(featureStats.stream().map(FeatureStatDTO::getFormulaId).toList());
        data.formulas = formulaService.getFormulasByIds(formulaIds);

        data.sequences = data.protocol.getFeatures().stream().collect(Collectors.groupingBy(FeatureDTO::getSequence));
        
        return data;
    }

    public static class ProtocolData {
    	public ProtocolDTO protocol;
    	public Map<Integer, List<FeatureDTO>> sequences;
    	public Map<Long, List<FeatureStatDTO>> featureStats;
    	public Map<Long, Formula> formulas;
    }
}

