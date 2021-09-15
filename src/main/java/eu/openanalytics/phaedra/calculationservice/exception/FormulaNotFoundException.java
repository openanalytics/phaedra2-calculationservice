package eu.openanalytics.phaedra.calculationservice.exception;

public class FormulaNotFoundException extends Exception {

    private final long requestedId;

    public FormulaNotFoundException(long requestedId) {
        this.requestedId = requestedId;
    }

    public String getMessage() {
        return String.format("Requested formula with id %s was not found", requestedId);
    }

}
