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
package eu.openanalytics.phaedra.calculationservice.execution.input;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import eu.openanalytics.phaedra.calculationservice.execution.CalculationContext;
import eu.openanalytics.phaedra.plateservice.dto.WellDTO;
import eu.openanalytics.phaedra.util.WellNumberUtils;

public class CalculationInputHelper {

	public static enum InputName {
		lowWellType,
		highWellType,
		wellNumbers,
		wellTypes,
		wellRows,
		wellColumns,
		wellStatus
	}

	public static void addWellInfo(Map<String, Object> inputMap, CalculationContext ctx) {
		if (ctx.getProtocolData() != null && ctx.getProtocolData().protocol != null) {
			inputMap.put(InputName.lowWellType.name(), ctx.getProtocolData().protocol.getLowWelltype());
			inputMap.put(InputName.highWellType.name(), ctx.getProtocolData().protocol.getHighWelltype());
		}

		if (ctx.getWells() != null) {
			// Sort wells by wellNumber
			List<WellDTO> wells = new ArrayList<>(ctx.getWells());
			int columnCount = ctx.getPlate().getColumns();
			wells.sort((w1, w2) -> {
				return WellNumberUtils.getWellNr(w1.getRow(), w1.getColumn(), columnCount) - WellNumberUtils.getWellNr(w2.getRow(), w2.getColumn(), columnCount);
			});

			inputMap.put(InputName.wellNumbers.name(), wells.stream().map(w -> WellNumberUtils.getWellNr(w.getRow(), w.getColumn(), columnCount)).toList());
			inputMap.put(InputName.wellTypes.name(), wells.stream().map(WellDTO::getWellType).toList());
			inputMap.put(InputName.wellRows.name(), wells.stream().map(WellDTO::getRow).toList());
			inputMap.put(InputName.wellColumns.name(), wells.stream().map(WellDTO::getColumn).toList());
			inputMap.put(InputName.wellStatus.name(), wells.stream().map(w -> w.getStatus().getCode()).toList());
		}
	}

	public static List<String> getReservedInputNames() {
		return Arrays.stream(InputName.values()).map(n -> n.name()).toList();
	}

	public static boolean isReservedInputName(String name) {
		return getReservedInputNames().contains(name);
	}
}
