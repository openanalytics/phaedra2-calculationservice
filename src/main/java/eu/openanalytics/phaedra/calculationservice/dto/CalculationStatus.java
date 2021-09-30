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
