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
package eu.openanalytics.phaedra.calculationservice.model;

import eu.openanalytics.phaedra.calculationservice.service.protocol.ErrorCollector;
import eu.openanalytics.phaedra.plateservice.dto.PlateDTO;
import eu.openanalytics.phaedra.plateservice.dto.WellDTO;
import eu.openanalytics.phaedra.plateservice.dto.WellSubstanceDTO;
import eu.openanalytics.phaedra.protocolservice.dto.FeatureDTO;
import lombok.*;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

@Data
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@Setter(AccessLevel.PRIVATE)
public class CurveFittingContext {
    @NonNull
    PlateDTO plate;

    @NonNull
    List<WellDTO> wells;

    @NonNull
    List<WellSubstanceDTO> wellSubstances;

    @NonNull
    List<String> uniqueSubstances;

    @NonNull
    List<FeatureDTO> curveFeatures;

    @NonNull
    Long resultSetId;

    public static CurveFittingContext newInstance(PlateDTO plate,
                                                  List<WellDTO> wells,
                                                 List<WellSubstanceDTO> wellSubstances,
                                                 List<String> uniqueSubstances,
                                                 List<FeatureDTO> curveFeatures,
                                                 Long resultSetId) {
        var res = new CurveFittingContext(plate, wells, wellSubstances, uniqueSubstances, curveFeatures, resultSetId);
        return res;
    }
}
