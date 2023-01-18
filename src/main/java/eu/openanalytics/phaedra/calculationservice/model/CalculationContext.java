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

import java.util.LinkedHashSet;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;

import eu.openanalytics.phaedra.calculationservice.service.protocol.ErrorCollector;
import eu.openanalytics.phaedra.plateservice.dto.PlateDTO;
import eu.openanalytics.phaedra.plateservice.dto.WellDTO;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NonNull;
import lombok.Setter;

@Data
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@Setter(AccessLevel.PRIVATE)
public class CalculationContext {

    @NonNull
    PlateDTO plate;

    @NonNull
    List<WellDTO> wells;

    @NonNull
    Protocol protocol;

    @NonNull
    Long resultSetId;

    @NonNull
    Long measId;

    ErrorCollector errorCollector;

    @NonNull
    List<String> welltypesSorted;

    @NonNull
    LinkedHashSet<String> uniqueWelltypes;

    int numWelltypes;

    @NonNull
    ConcurrentHashMap<Feature, Future<Boolean>> computedStatsForFeature;

    public static CalculationContext newInstance(PlateDTO plate,
                                                 List<WellDTO> wells,
                                                 Protocol protocol,
                                                 Long resultSetId,
                                                 Long measId,
                                                 List<String> welltypesSorted,
                                                 LinkedHashSet<String> uniqueWelltypes) {
        var res = new CalculationContext(plate, wells, protocol, resultSetId, measId,
                null, welltypesSorted, uniqueWelltypes, uniqueWelltypes.size(),
                new ConcurrentHashMap<>());
        res.errorCollector = new ErrorCollector(res);
        return res;
    }

}
