package eu.openanalytics.phaedra.calculationservice.service;

import eu.openanalytics.phaedra.calculationservice.model.CalculationInputValue;
import eu.openanalytics.phaedra.calculationservice.model.Feature;
import eu.openanalytics.phaedra.calculationservice.scriptengineclient.model.ScriptExecutionOutput;
import lombok.Builder;
import lombok.NonNull;
import lombok.Value;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ErrorCollector {


    @Value
    @Builder
    public static class Error {
        @NonNull
        LocalDateTime timestamp;
        String exceptionClassName;
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

    private final List<Error> errors = Collections.synchronizedList(new ArrayList<>());

    public String getErrorDescription() {
        StringBuilder description = new StringBuilder();
        for (var error : errors) {
            description.append(String.format(" - Timestamp: [%s]", error.getTimestamp()));
            if (error.getExceptionClassName() != null) {
                description.append(String.format(", Exception: [%s]", error.getExceptionClassName()));
            }
            description.append(String.format(", Description: [%s]", error.getDescription()));
            if (error.getFeatureId() != null) {
                description.append(String.format(", Feature: [%s %s], Sequence: [%s], Formula: [%s %s]", error.getFeatureId(), error.getFeatureName(), error.getSequenceNumber(), error.getFormulaId(), error.getFormulaName()));
            }
            if (error.getCivType() != null) {
                description.append(String.format(", CivType: [%s], CivSource: [%s], CivVariableName: [%s]", error.getCivType(), error.getCivSource(), error.getCivVariableName()));
            }
            if (error.getExitCode() != null) {
                description.append(String.format(", ExitCode: [%s]", error.getExitCode()));
            }
            if (error.getStatusMessage() != null) {
                description.append(String.format(", StatusMessage: [%s]", error.getStatusMessage()));
            }
            description.append("\n");
        }
        return description.toString();
    }

    public boolean hasError() {
        return errors.size() > 0;
    }

    public void handleError(Throwable e, String description, Feature feature) {
        errors.add(Error.builder()
                .timestamp(LocalDateTime.now())
                .exceptionClassName(e.getClass().getSimpleName())
                .description(description)
                .sequenceNumber(feature.getSequence())
                .featureId(feature.getId())
                .featureName(feature.getName())
                .formulaId(feature.getFormula().getId())
                .formulaName(feature.getFormula().getName())
                .build()
        );
    }

    public void handleError(Exception e, String description, Feature feature, CalculationInputValue civ) {
        errors.add(Error.builder()
                .timestamp(LocalDateTime.now())
                .exceptionClassName(e.getClass().getSimpleName())
                .description(description)
                .sequenceNumber(feature.getSequence())
                .featureId(feature.getId())
                .featureName(feature.getName())
                .formulaId(feature.getFormula().getId())
                .formulaName(feature.getFormula().getName())
                .civType(civ.getType())
                .civVariableName(civ.getVariableName())
                .civSource(civ.getSource())
                .build());
    }

    public void handleError(String description, Feature feature, CalculationInputValue civ) {
        handleError(null, description, feature, civ);
    }

    public void handleError(Exception e, String description, Feature feature) {
        errors.add(Error.builder()
                .exceptionClassName(e.getClass().getSimpleName())
                .timestamp(LocalDateTime.now())
                .description(description)
                .featureId(feature.getId())
                .featureName(feature.getName())
                .sequenceNumber(feature.getSequence())
                .formulaId(feature.getFormula().getId())
                .formulaName(feature.getFormula().getName())
                .build());
    }

    public void handleError(String description, Feature feature) {
        handleError(null, description, feature);
    }


    public void handleScriptError(ScriptExecutionOutput output, Feature feature) {
        errors.add(Error.builder()
                .timestamp(LocalDateTime.now())
                .description("executing sequence => processing output => output indicates script error")
                .sequenceNumber(feature.getSequence())
                .featureId(feature.getId())
                .featureName(feature.getName())
                .formulaId(feature.getFormula().getId())
                .formulaName(feature.getFormula().getName())
                .exitCode(output.getExitCode())
                .statusMessage(output.getStatusMessage())
                .build());
    }
}
