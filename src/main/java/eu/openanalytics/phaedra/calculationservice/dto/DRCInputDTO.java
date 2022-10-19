package eu.openanalytics.phaedra.calculationservice.dto;

import eu.openanalytics.phaedra.protocolservice.dto.DRCModelDTO;
import lombok.*;

import java.util.Optional;

@Value
@Builder
@AllArgsConstructor
@NoArgsConstructor(force = true, access = AccessLevel.PRIVATE) // Jackson deserialize compatibility
public class DRCInputDTO {
    String substance;
    long plateId;
    long featureId;
    long protocolId;
    long resultSetId;
    long[] wells;
    float[] values;
    float[] concs;
    float[] accepts;
    Optional<DRCModelDTO> drcModel;
}
