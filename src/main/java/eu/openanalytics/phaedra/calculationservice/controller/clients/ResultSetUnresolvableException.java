package eu.openanalytics.phaedra.calculationservice.controller.clients;

public class ResultSetUnresolvableException extends Exception {
    public ResultSetUnresolvableException(String msg) {
        super(msg);
    }

    public ResultSetUnresolvableException(String msg, Throwable cause) {
        super(msg, cause);
    }
}
