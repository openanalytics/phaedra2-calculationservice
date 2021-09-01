package eu.openanalytics.phaedra.calculationservice.model;

import eu.openanalytics.phaedra.calculationservice.enumeration.FeatureType;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Value;

import java.util.List;

@Value
@Builder(toBuilder = true)
@AllArgsConstructor
public class Feature {

    Long id;

    String name;

    String alias;

    String description;

    String format;

    FeatureType type;

    Integer sequence;

    Formula formula;

    List<CalculationInputValue> calculationInputValues;

}
