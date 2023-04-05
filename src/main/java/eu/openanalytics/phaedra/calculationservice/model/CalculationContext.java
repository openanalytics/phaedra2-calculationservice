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
package eu.openanalytics.phaedra.calculationservice.model;

import java.util.List;

import eu.openanalytics.phaedra.calculationservice.service.protocol.ProtocolDataCollector.ProtocolData;
import eu.openanalytics.phaedra.calculationservice.util.CalculationProgress;
import eu.openanalytics.phaedra.calculationservice.util.ErrorCollector;
import eu.openanalytics.phaedra.plateservice.dto.PlateDTO;
import eu.openanalytics.phaedra.plateservice.dto.WellDTO;
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

    ErrorCollector errorCollector;
    CalculationProgress calculationProgress;
    
    public static CalculationContext newInstance(ProtocolData protocolData, PlateDTO plate, List<WellDTO> wells, Long resultSetId, Long measId) {
    	CalculationContext ctx = new CalculationContext(protocolData, plate, wells, resultSetId, measId, null, null);
    	ctx.calculationProgress = new CalculationProgress(ctx);
        ctx.errorCollector = new ErrorCollector(ctx);
        return ctx;
    }

}
