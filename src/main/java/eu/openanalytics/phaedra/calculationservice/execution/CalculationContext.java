/**
 * Phaedra II
 *
 * Copyright (C) 2016-2024 Open Analytics
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
package eu.openanalytics.phaedra.calculationservice.execution;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import eu.openanalytics.phaedra.calculationservice.execution.progress.CalculationStateTracker;
import eu.openanalytics.phaedra.calculationservice.service.protocol.ProtocolDataCollector.ProtocolData;
import eu.openanalytics.phaedra.calculationservice.util.ErrorCollector;
import eu.openanalytics.phaedra.plateservice.dto.PlateDTO;
import eu.openanalytics.phaedra.plateservice.dto.WellDTO;
import eu.openanalytics.phaedra.resultdataservice.dto.ResultDataDTO;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Setter;

@Data
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@Setter(AccessLevel.PRIVATE)
public class CalculationContext {

	ProtocolData protocolData;
	
    PlateDTO plate;
    List<WellDTO> wells;
    
    Long resultSetId;
    Long measId;

    Map<Long, ResultDataDTO> featureResults;
    Map<String, Map<Integer, float[]>> subwellDataCache;
    
    ErrorCollector errorCollector;
    CalculationStateTracker stateTracker;
    
    public static CalculationContext create(ProtocolData protocolData, PlateDTO plate, List<WellDTO> wells, Long resultSetId, Long measId) {
    	CalculationContext ctx = new CalculationContext(protocolData, plate, wells, resultSetId, measId, null, null, null, null);
    	ctx.stateTracker = new CalculationStateTracker(ctx);
        ctx.errorCollector = new ErrorCollector(ctx);
        ctx.featureResults = new HashMap<>();
        ctx.subwellDataCache = Collections.synchronizedMap(new HashMap<>());
        return ctx;
    }

}
