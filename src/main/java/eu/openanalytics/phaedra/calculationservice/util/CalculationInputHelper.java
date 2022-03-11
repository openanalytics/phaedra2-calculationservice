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
package eu.openanalytics.phaedra.calculationservice.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import eu.openanalytics.phaedra.calculationservice.model.CalculationContext;
import eu.openanalytics.phaedra.plateservice.dto.WellDTO;
import eu.openanalytics.phaedra.util.WellNumberUtils;

public class CalculationInputHelper {

	public static void addWellInfo(Map<String, Object> inputMap, CalculationContext context) {
		inputMap.put("lowWellType", context.getProtocol().getLowWelltype());
		inputMap.put("highWellType", context.getProtocol().getHighWelltype());
		
		List<WellDTO> wells = new ArrayList<>(context.getPlate().getWells());
		
		// Sort wells by wellNumber
		int columnCount = context.getPlate().getColumns();
		wells.sort((w1, w2) -> { 
			return WellNumberUtils.getWellNr(w1.getRow(), w1.getColumn(), columnCount) - WellNumberUtils.getWellNr(w2.getRow(), w2.getColumn(), columnCount);
		});
		
		inputMap.put("wellNumbers", wells.stream().map(w -> WellNumberUtils.getWellNr(w.getRow(), w.getColumn(), columnCount)).toList());
		inputMap.put("wellTypes", wells.stream().map(WellDTO::getWellType).toList());
		inputMap.put("wellRows", wells.stream().map(WellDTO::getRow).toList());
		inputMap.put("wellColumns", wells.stream().map(WellDTO::getColumn).toList());
		inputMap.put("wellStatus", wells.stream().map(w -> w.getStatus().getCode()).toList());
	}
}
