package eu.openanalytics.phaedra.calculationservice.controller.clients.impl;

public class UrlFactory {

    private static final String MEAS_SERVICE = "http://phaedra-measurement-service/phaedra/meas-service";

    public static String measurementWell(long measId, String columnName) {
        return String.format("%s/meas/%s/welldata/%s", MEAS_SERVICE, measId, columnName);
    }

}
