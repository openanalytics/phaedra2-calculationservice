package eu.openanalytics.phaedra.calculationservice.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Value;
import lombok.With;

@Value
@With
@Builder(toBuilder = true)
@AllArgsConstructor
public class FeatureStat {

    Long id;

    Long featureId;

    Boolean plateStat;

    Boolean welltypeStat;

    String name;

    Formula formula;

}
