package eu.openanalytics.phaedra.calculationservice.scriptengineclient.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Value;

/**
 * The output of the execution a script.
 */
@Value
public class ScriptExecutionOutput {

    String inputId;
    String output;
    ResponseStatusCode statusCode;
    String statusMessage;
    int exitCode;

    public ScriptExecutionOutput(
            @JsonProperty(value = "input_id", required = true) String inputId,
            @JsonProperty(value = "output", required = true) String output,
            @JsonProperty(value = "status_code", required = true) ResponseStatusCode statusCode,
            @JsonProperty(value = "status_message", required = true) String statusMessage,
            @JsonProperty(value = "exit_code", required = true) int exitCode) {
        this.inputId = inputId;
        this.output = output;
        this.statusCode = statusCode;
        this.statusMessage = statusMessage;
        this.exitCode = exitCode;
    }

//    public String getOutput() {
//        return output;
//    }
//
//    public ResponseStatusCode getStatusCode() {
//        return statusCode;
//    }
//
//    public String getStatusMessage() {
//        return statusMessage;
//    }
//
//    public int getExitCode() {
//        return exitCode;
//    }
//
//    public String getInputId() {
//        return inputId;
//    }
}
