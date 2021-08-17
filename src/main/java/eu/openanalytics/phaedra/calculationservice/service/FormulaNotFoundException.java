package eu.openanalytics.phaedra.calculationservice.service;

public class FormulaNotFoundException extends Exception{

    private final long requestedId;

    FormulaNotFoundException(long requestedId) {
        this.requestedId = requestedId;
    }

    public String getMessage() {
        return String.format("Requested formula with id %s was not found", requestedId);
    }

}
