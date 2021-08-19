package eu.openanalytics.phaedra.calculationservice.dto.external;

import eu.openanalytics.phaedra.calculationservice.enumeration.FeatureType;
import lombok.Data;

@Data
public class FeatureDTO {
    private Long id;
    private String name;
    private String alias;
    private String description;
    private String format;
    private FeatureType type;
    private Integer sequence;
    private Long formula;
}
