package eu.openanalytics.phaedra.calculationservice.dto.external;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.Value;

import java.time.LocalDateTime;

@Value
@Builder
@AllArgsConstructor
@NoArgsConstructor(force = true, access = AccessLevel.PRIVATE) // Jackson deserialize compatibility
public class ErrorDTO {

    @NonNull
    LocalDateTime timestamp;
    // TODO excpetion message + stacktrace?
    String exceptionClassName;
    String exceptionMessage;
    @NonNull
    String description;
    Long featureId;
    String featureName;
    Integer sequenceNumber;
    Long formulaId;
    String formulaName;
    String civType;
    String civVariableName;
    String civSource;
    Integer exitCode;
    String statusMessage;

}