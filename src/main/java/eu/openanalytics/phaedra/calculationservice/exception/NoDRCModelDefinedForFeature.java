package eu.openanalytics.phaedra.calculationservice.exception;

public class NoDRCModelDefinedForFeature extends RuntimeException {

    public NoDRCModelDefinedForFeature(String message) {
        super(message);
    }

    public NoDRCModelDefinedForFeature(String msg, Object... args) {
        super(String.format(msg, args));
    }
}
