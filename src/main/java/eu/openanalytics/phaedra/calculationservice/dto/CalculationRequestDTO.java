package eu.openanalytics.phaedra.calculationservice.dto;

import lombok.*;

@Builder
@Value
@AllArgsConstructor
@NoArgsConstructor(force = true, access = AccessLevel.PRIVATE) // Jackson deserialize compatibility
public class CalculationRequestDTO {

    long protocolId;
    long plateId;
    long measId;

}
