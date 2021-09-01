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
