package eu.openanalytics.phaedra.calculationservice.model;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.Value;
import org.springframework.data.annotation.Id;

//@Data
@Value
@NoArgsConstructor(force = true)
@AllArgsConstructor
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
