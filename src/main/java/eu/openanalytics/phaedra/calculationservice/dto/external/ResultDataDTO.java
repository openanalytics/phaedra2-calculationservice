package eu.openanalytics.phaedra.calculationservice.dto.external;

import eu.openanalytics.phaedra.calculationservice.scriptengineclient.model.ResponseStatusCode;
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
public class ResultDataDTO {

    Long id;

    Long resultSetId;

    Long featureId;

    float[] values;

    ResponseStatusCode statusCode;

    String statusMessage;

    Integer exitCode;

    LocalDateTime createdTimestamp;

}
