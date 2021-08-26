package eu.openanalytics.phaedra.calculationservice.controller.clients;

public class ResultDataUnresolvableException extends Exception {
    public ResultDataUnresolvableException(String msg) {
        super(msg);
    }

    public ResultDataUnresolvableException(String msg, Throwable cause) {
        super(msg, cause);
    }
}
