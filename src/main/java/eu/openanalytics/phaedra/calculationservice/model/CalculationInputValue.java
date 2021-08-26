package eu.openanalytics.phaedra.calculationservice.model;

import lombok.Data;
import org.springframework.data.annotation.Id;

@Data
public class CalculationInputValue {

    @Id
    private Long id;
    private Long featureId;
    private String sourceMeasColName;
    private Long sourceFeatureId;
    private String variableName;

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
