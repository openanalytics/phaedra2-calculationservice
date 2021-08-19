package eu.openanalytics.phaedra.calculationservice.dto.external;

import lombok.Data;

@Data
public class CalculationInputValueDTO {

    private Long id;

    private Long featureId;

    private String sourceMeasColName;
    private String sourceFeatureId;
    private String variableName;

}
