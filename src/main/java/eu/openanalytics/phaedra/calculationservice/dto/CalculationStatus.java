package eu.openanalytics.phaedra.calculationservice.dto;

import eu.openanalytics.phaedra.calculationservice.enumeration.CalculationStatusCode;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.NoArgsConstructor;
import lombok.Value;

import java.util.Map;

@Value
@Builder
@AllArgsConstructor
@NoArgsConstructor(force = true, access = AccessLevel.PRIVATE) // Jackson deserialize compatibility
public class CalculationStatus {

    CalculationComplexityDTO complexity;

    Map<Integer, SequenceStatusDTO> sequences;

    @Value
    @Builder
    @AllArgsConstructor
    @NoArgsConstructor(force = true, access = AccessLevel.PRIVATE) // Jackson deserialize compatibility
    public static class CalculationComplexityDTO {
        int steps;
        int features;
        int featureStats;
        int sequences;
    }

    @Value
    @Builder
    @AllArgsConstructor
    @NoArgsConstructor(force = true, access = AccessLevel.PRIVATE) // Jackson deserialize compatibility
    public static class SequenceStatusDTO {
        CalculationStatusCode statusCode;
        Map<Long, FeatureStatusDTO> features;
    }

    @Value
    @Builder
    @AllArgsConstructor
    @NoArgsConstructor(force = true, access = AccessLevel.PRIVATE) // Jackson deserialize compatibility
    public static class FeatureStatusDTO {
        CalculationStatusCode statusCode;
        CalculationStatusCode statsStatusCode;
        Map<Long, CalculationStatusCode> stats;
    }

}
