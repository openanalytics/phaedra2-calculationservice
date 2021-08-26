package eu.openanalytics.phaedra.calculationservice.dto.external;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.NoArgsConstructor;
import lombok.Value;
import lombok.With;

import java.time.LocalDateTime;

@Value
@Builder
@With
@AllArgsConstructor
@NoArgsConstructor(force = true, access = AccessLevel.PRIVATE) // Jackson deserialize compatibility
public class ResultSetDTO {

    Long id;

    Long protocolId;

    Long plateId;

    Long measId;

    LocalDateTime executionStartTimeStamp;

    LocalDateTime executionEndTimeStamp;

    String outcome;

}
