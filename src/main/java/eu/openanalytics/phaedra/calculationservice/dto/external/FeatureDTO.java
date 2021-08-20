package eu.openanalytics.phaedra.calculationservice.dto.external;

import eu.openanalytics.phaedra.calculationservice.enumeration.FeatureType;
import eu.openanalytics.phaedra.calculationservice.enumeration.ScriptLanguage;
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
    private Long protocolId;
    private Long formulaId;
    private String trigger;
    private ScriptLanguage scriptLanguage;
}
