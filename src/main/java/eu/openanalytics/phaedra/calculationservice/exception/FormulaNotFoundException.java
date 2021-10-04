package eu.openanalytics.phaedra.calculationservice.exception;

import eu.openanalytics.phaedra.util.exceptionhandling.EntityNotFoundException;

public class FormulaNotFoundException extends EntityNotFoundException {

    public FormulaNotFoundException(long requestedId) {
        super(String.format("Requested formula with id %s was not found", requestedId));
    }

}
