package eu.openanalytics.phaedra.calculationservice.dto;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.Value;

@Value
@NoArgsConstructor(force = true, access = AccessLevel.PRIVATE) // Jackson deserialize compatibility
public class CalculationRequestDTO {

    long protocolId;
    long plateId;
    long measId;

}
