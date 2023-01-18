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

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Value;
import org.springframework.data.annotation.Id;

@Value
@AllArgsConstructor
@Builder(toBuilder = true)
public class CalculationInputValue {

    @Id
    Long id;
    Long featureId;
    String sourceMeasColName;
    Long sourceFeatureId;
    String variableName;

    public String getType() {
        if (sourceMeasColName != null) {
            return "fromMeasurement";
        } else if (sourceFeatureId != null) {
            return "fromFeature";
        }
        return null;
    }

    public String getSource() {
        if (sourceMeasColName != null) {
            return sourceMeasColName;
        } else if (sourceFeatureId != null) {
            return String.valueOf(sourceFeatureId);
        }
        return null;
    }

}
