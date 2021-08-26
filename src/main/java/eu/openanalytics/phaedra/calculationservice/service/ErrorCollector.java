package eu.openanalytics.phaedra.calculationservice.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import eu.openanalytics.phaedra.calculationservice.controller.clients.MeasUnresolvableException;
import eu.openanalytics.phaedra.calculationservice.controller.clients.ResultDataUnresolvableException;
import eu.openanalytics.phaedra.calculationservice.model.CalculationInputValue;
import eu.openanalytics.phaedra.calculationservice.model.Feature;
import eu.openanalytics.phaedra.calculationservice.model.Formula;
import eu.openanalytics.phaedra.calculationservice.scriptengineclient.model.ScriptExecutionOutput;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class ErrorCollector {

    // TODO MAKES THIS TRHEAD_SAFE!!!!
    private final List<Throwable> errors = new ArrayList<>();
    private final List<String> messages = new ArrayList<>();

    public ErrorCollector() {

    }

//    public void handleError(InterruptedException e) {
//        errors.add(e);
//        messages.add(e.getMessage());
//    }
//
//    public void handleError(MeasUnresolvableException e) {
//        errors.add(e);
//        messages.add(e.getMessage());
//    }
//
//    public void handleError(ExecutionException e) {
//        errors.add(e);
//        messages.add(e.getMessage());
//    }

    public void handleError(ResultDataUnresolvableException e) {
        errors.add(e);
        messages.add(e.getMessage());
    }

    public String getErrorDescription() {
        return String.join("\n", messages);
    }

    public boolean hasError() {
        return messages.size() > 0;
    }

    public void handleError(Throwable e, String location, Feature feature) {
        errors.add(e);
        messages.add(String.format("Type: [%s], location: [%s], sequenceNumber: [%s], feature: [%s=>%s]",
                e.getClass().getSimpleName(), location, feature.getSequence(), feature.getId(), feature.getName()));
    }

    public void handleError(Exception e, String location, Feature feature, CalculationInputValue civ) {
        if (e == null) {
            messages.add(String.format("Location: [%s], sequenceNumber: [%s], feature: [%s=>%s], civType: [%s], civVariableName: [%s], civSource: [%s]",
                    location, feature.getSequence(), feature.getId(), feature.getName(), civ.getType(), civ.getVariableName(), civ.getSource()));
        } else {
            messages.add(String.format("Type: [%s], location: [%s], sequenceNumber: [%s], feature: [%s=>%s], civType: [%s], civVariableName: [%s], civSource: [%s]",
                    e.getClass().getSimpleName(), location, feature.getSequence(), feature.getId(), feature.getName(), civ.getType(), civ.getVariableName(), civ.getSource()));
        }
    }

    public void handleError(Exception e, String location, Feature feature) {
        if (e == null) {
            messages.add(String.format("Location: [%s], sequenceNumber: [%s], feature: [%s=>%s], formula: [%s=>%s]",
                    location, feature.getSequence(), feature.getId(), feature.getName(), feature.getFormula().getId(), feature.getFormula().getName()));
        } else {
            messages.add(String.format("Type: [%s], Location: [%s], sequenceNumber: [%s], feature: [%s=>%s], formula: [%s=>%s]",
                    e.getClass().getSimpleName(), location, feature.getSequence(), feature.getId(), feature.getName(), feature.getFormula().getId(), feature.getFormula().getName()));
        }
    }

    public void handleScriptError(ScriptExecutionOutput output, Feature feature) {
        messages.add(String.format("Location: [execution sequence => processing output => output indicates script error], sequenceNumber: [%s], feature: [%s=>%s], formula: [%s=>%s], exitCode: [%s], statusMessage of script: [%s]",
                feature.getSequence(), feature.getId(), feature.getName(), feature.getFormula().getId(), feature.getFormula().getName(), output.getExitCode(), output.getStatusMessage()));
    }
}
