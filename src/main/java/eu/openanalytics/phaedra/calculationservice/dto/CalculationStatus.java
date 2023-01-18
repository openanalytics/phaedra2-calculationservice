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
package eu.openanalytics.phaedra.calculationservice.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import eu.openanalytics.phaedra.calculationservice.enumeration.CalculationStatusCode;
import eu.openanalytics.phaedra.resultdataservice.dto.ErrorDTO;
import eu.openanalytics.phaedra.resultdataservice.enumeration.StatusCode;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.Value;

import java.util.List;
import java.util.Map;

@Value
@Builder
@AllArgsConstructor
@NoArgsConstructor(force = true, access = AccessLevel.PRIVATE) // Jackson deserialize compatibility
public class CalculationStatus {

    CalculationComplexityDTO complexity;

    StatusCode statusCode;

    List<ErrorDTO> errors;

    Map<Integer, SequenceStatusDTO> sequences;

    @Value
    @Builder
    @AllArgsConstructor
    @NoArgsConstructor(force = true, access = AccessLevel.PRIVATE) // Jackson deserialize compatibility
    public static class CalculationComplexityDTO {
        int steps;
        int features;
        int featureStats;
        int featureStatResults;
        int sequences;
    }

    @Value
    @Builder
    @AllArgsConstructor
    @NoArgsConstructor(force = true, access = AccessLevel.PRIVATE) // Jackson deserialize compatibility
    public static class SequenceStatusDTO {
        StatusDescription status;
        Map<Long, FeatureStatusDTO> features;
    }

    @Value
    @Builder
    @AllArgsConstructor
    @NoArgsConstructor(force = true, access = AccessLevel.PRIVATE) // Jackson deserialize compatibility
    public static class FeatureStatusDTO {
        StatusDescription status;
        StatusDescription statStatus;
        Map<Long, StatusDescription> stats;
    }

    @Value
    @Builder
    @AllArgsConstructor
    @NoArgsConstructor(force = true, access = AccessLevel.PRIVATE) // Jackson deserialize compatibility
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class StatusDescription {
        @NonNull
        CalculationStatusCode statusCode;
        String statusMessage;
        String description;

        public StatusDescription(CalculationStatusCode statusCode) {
            this.statusCode = statusCode;
            this.statusMessage = null;
            this.description = null;
        }
    }

}
