package eu.openanalytics.phaedra.calculationservice.service.protocol;

import eu.openanalytics.phaedra.calculationservice.model.CalculationInputValue;
import eu.openanalytics.phaedra.calculationservice.model.Feature;
import eu.openanalytics.phaedra.calculationservice.model.FeatureStat;
import eu.openanalytics.phaedra.calculationservice.model.Formula;
import eu.openanalytics.phaedra.resultdataservice.dto.ErrorDTO;
import eu.openanalytics.phaedra.scriptengine.dto.ScriptExecutionOutputDTO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

// TODO keep track of state
public class ErrorCollector {

    private final List<ErrorDTO> errors = Collections.synchronizedList(new ArrayList<>());
    private final Logger logger = LoggerFactory.getLogger(getClass());

    public List<ErrorDTO> getErrors() {
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

    public void handleError(String description, Object... ctxObjects) {
        var errorBuilder = ErrorDTO.builder()
                .timestamp(LocalDateTime.now())
                .description(description);

        Optional<Throwable> exception = Optional.empty();

        for (Object ctx : ctxObjects) {
            if (ctx instanceof Feature feature) {
                errorBuilder
                        .sequenceNumber(feature.getSequence())
                        .featureId(feature.getId())
                        .featureName(feature.getName());
            } else if (ctx instanceof ScriptExecutionOutputDTO output) {
                errorBuilder
                        .exitCode(output.getExitCode())
                        .statusMessage(output.getStatusMessage());
            } else if (ctx instanceof CalculationInputValue civ) {
                errorBuilder
                        .civType(civ.getType())
                        .civVariableName(civ.getVariableName())
                        .civSource(civ.getSource());
            } else if (ctx instanceof FeatureStat featureStat) {
                errorBuilder
                        .featureStatId(featureStat.getId())
                        .featureStatName(featureStat.getName());
            } else if (ctx instanceof Formula formula) {
                errorBuilder
                        .formulaId(formula.getId())
                        .formulaName(formula.getName());
            } else if (ctx instanceof Throwable e) {
                errorBuilder
                        .exceptionClassName(e.getClass().getSimpleName())
                        .exceptionMessage(e.getMessage());
                if (exception.isPresent()) {
                    logger.warn("Multiple exception provided to errorCollector:handleError");
                }
                exception = Optional.of(e);
            } else {
                logger.warn("Unrecognized contextObject passed to errorCollector:handleError");
            }
        }

        var error = errorBuilder.build();
        errors.add(error);
        if (exception.isPresent()) {
            logger.info(error.toString(), exception);
        } else {
            logger.info(error.toString());
        }
    }

    public void handleError(String description, int sequenceNumber) {
        var error = ErrorDTO.builder()
                .timestamp(LocalDateTime.now())
                .description(description)
                .sequenceNumber(sequenceNumber)
                .build();
        errors.add(error);
    }

}
