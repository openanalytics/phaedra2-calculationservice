/**
 * Phaedra II
 *
 * Copyright (C) 2016-2025 Open Analytics
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

public class CalculationInputHelper {

	public static enum InputName {
		lowWellType,
		highWellType,
		wellNumbers,
		wellTypes,
		wellRows,
		wellColumns,
		wellStatus,
		doses,
		responses,
		accepts
	}

	public static void addWellInfo(Map<String, Object> inputMap, CalculationContext ctx, List<WellDTO> wells) {
		if (ctx.getProtocolData() != null && ctx.getProtocolData().protocol != null) {
			inputMap.put(InputName.lowWellType.name(), ctx.getProtocolData().protocol.getLowWelltype());
			inputMap.put(InputName.highWellType.name(), ctx.getProtocolData().protocol.getHighWelltype());
		}

		if (wells != null) {
			List<WellDTO> sortedWells = sortByNr(wells);
			inputMap.put(InputName.wellNumbers.name(), sortedWells.stream().map(WellDTO::getWellNr).toList());
			inputMap.put(InputName.wellTypes.name(), sortedWells.stream().map(WellDTO::getWellType).toList());
			inputMap.put(InputName.wellRows.name(), sortedWells.stream().map(WellDTO::getRow).toList());
			inputMap.put(InputName.wellColumns.name(), sortedWells.stream().map(WellDTO::getColumn).toList());
			inputMap.put(InputName.wellStatus.name(), sortedWells.stream().map(w -> w.getStatus().getCode()).toList());
		}
	}

	public static List<String> getReservedInputNames() {
		return Arrays.stream(InputName.values()).map(n -> n.name()).toList();
	}

	public static boolean isReservedInputName(String name) {
		return getReservedInputNames().contains(name);
	}

	public static List<WellDTO> sortByNr(List<WellDTO> wells) {
		List<WellDTO> sortedWells = new ArrayList<>(wells);
		sortedWells.sort((w1, w2) -> w1.getWellNr() - w2.getWellNr());
		return sortedWells;
	}
}
