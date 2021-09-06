package eu.openanalytics.phaedra.calculationservice.dto.external;


import lombok.Data;

import java.util.List;

@Data
public class ProtocolDTO {
    private Long id;
    private String name;
    private String description;
    private boolean editable;
    private boolean inDevelopment;
    private String lowWelltype;
    private String highWelltype;
    private List<FeatureDTO> features;
}
