package eu.openanalytics.phaedra.calculationservice.service;

import eu.openanalytics.phaedra.calculationservice.model.CalculationInputValue;
import eu.openanalytics.phaedra.calculationservice.model.Error;
import eu.openanalytics.phaedra.calculationservice.model.Feature;
import eu.openanalytics.phaedra.calculationservice.scriptengineclient.model.ScriptExecutionOutput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

// TODO store in DB
// TODO keep track of state
public class ErrorCollector {

    private final List<Error> errors = Collections.synchronizedList(new ArrayList<>());
    private final Logger logger = LoggerFactory.getLogger(getClass());

    public List<Error> getErrors() {
        return errors;
    }

    public String getErrorDescription() {
        StringBuilder description = new StringBuilder();
        for (var error : errors) {
            description.append(error);
            description.append("\n");
        }
        return description.toString();
    }

    public boolean hasError() {
        return errors.size() > 0;
    }

    public void handleError(Throwable e, String description, Feature feature) {
        var error = Error.builder()
                .timestamp(LocalDateTime.now())
                .exceptionClassName(e.getClass().getSimpleName())
                .exceptionMessage(e.getMessage())
                .description(description)
                .sequenceNumber(feature.getSequence())
                .featureId(feature.getId())
                .featureName(feature.getName())
                .formulaId(feature.getFormula().getId())
                .formulaName(feature.getFormula().getName())
                .build();
        errors.add(error);
        logger.info(error.toString(), e);
    }

    public void handleError(Exception e, String description, Feature feature, CalculationInputValue civ) {
        var error = Error.builder()
                .timestamp(LocalDateTime.now())
                .exceptionClassName(e.getClass().getSimpleName())
                .exceptionMessage(e.getMessage())
                .description(description)
                .sequenceNumber(feature.getSequence())
                .featureId(feature.getId())
                .featureName(feature.getName())
                .formulaId(feature.getFormula().getId())
                .formulaName(feature.getFormula().getName())
                .civType(civ.getType())
                .civVariableName(civ.getVariableName())
                .civSource(civ.getSource())
                .build();
        errors.add(error);
        logger.info(error.toString(), e);
    }

    public void handleError(String description, Feature feature, CalculationInputValue civ) {
        var error = Error.builder()
                .timestamp(LocalDateTime.now())
                .description(description)
                .featureId(feature.getId())
                .featureName(feature.getName())
                .sequenceNumber(feature.getSequence())
                .formulaId(feature.getFormula().getId())
                .formulaName(feature.getFormula().getName())
                .build();
        errors.add(error);
        logger.info(error.toString());
    }

    public void handleError(Exception e, String description, Feature feature) {
        var error = Error.builder()
                .exceptionClassName(e.getClass().getSimpleName())
                .exceptionMessage(e.getMessage())
                .timestamp(LocalDateTime.now())
                .description(description)
                .featureId(feature.getId())
                .featureName(feature.getName())
                .sequenceNumber(feature.getSequence())
                .formulaId(feature.getFormula().getId())
                .formulaName(feature.getFormula().getName())
                .build();
        errors.add(error);
        logger.info(error.toString(), e);
    }

    public void handleError(String description, Feature feature) {
        var error = Error.builder()
                .timestamp(LocalDateTime.now())
                .description(description)
                .featureId(feature.getId())
                .featureName(feature.getName())
                .sequenceNumber(feature.getSequence())
                .formulaId(feature.getFormula().getId())
                .formulaName(feature.getFormula().getName())
                .build();
        errors.add(error);
        logger.info(error.toString());
    }


    public void handleScriptError(ScriptExecutionOutput output, Feature feature) {
        var error = Error.builder()
                .timestamp(LocalDateTime.now())
                .description("executing sequence => processing output => output indicates script error")
                .sequenceNumber(feature.getSequence())
                .featureId(feature.getId())
                .featureName(feature.getName())
                .formulaId(feature.getFormula().getId())
                .formulaName(feature.getFormula().getName())
                .exitCode(output.getExitCode())
                .statusMessage(output.getStatusMessage())
                .build();
        errors.add(error);
        logger.info(error.toString());
    }
}
