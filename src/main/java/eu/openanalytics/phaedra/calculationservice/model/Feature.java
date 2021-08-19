package eu.openanalytics.phaedra.calculationservice.model;

import eu.openanalytics.phaedra.calculationservice.enumeration.FeatureType;
import lombok.Value;

@Value
public class Feature {

    Long id;

    String name;

    String alias;

    String description;

    String format;

    FeatureType type;

    Integer sequence;

    Formula formula;
}
