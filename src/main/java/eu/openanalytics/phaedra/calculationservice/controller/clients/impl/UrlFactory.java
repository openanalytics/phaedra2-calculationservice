package eu.openanalytics.phaedra.calculationservice.controller.clients.impl;

public class UrlFactory {

    private static final String PROTOCOL_SERVICE = "http://phaedra-protocol-service/phaedra/protocol-service";
    private static final String RESULTDATA_SERVICE = "http://phaedra-resultdata-service/phaedra/resultdata-service";
    private static final String MEAS_SERVICE = "http://127.0.0.1:3008";

    public static String protocol(long protocolId) {
        return String.format("%s/protocols/%s", PROTOCOL_SERVICE, protocolId);
    }

    public static String protocolFeatures(long protocolId) {
        return String.format("%s/protocols/%s/features", PROTOCOL_SERVICE, protocolId);
    }

    public static String protocolCiv(long protocolId) {
        return String.format("%s/protocols/%s/calculationinputvalue", PROTOCOL_SERVICE, protocolId);
    }

    public static String measurementWell(long measId, String columnName) {
        return String.format("%s/meas/%s/welldata/%s", MEAS_SERVICE, measId, columnName);
    }

    public static String resultSet() {
        return String.format("%s/resultset", RESULTDATA_SERVICE);
    }

    public static String resultSet(long resultId) {
        return String.format("%s/resultset/%s", RESULTDATA_SERVICE, resultId);
    }

    public static String resultData(long resultSetId) {
        return String.format("%s/resultset/%s/resultdata", RESULTDATA_SERVICE, resultSetId);
    }

    public static String resultDataByFeatureId(long resultSetId, long featureId) {
        return String.format("%s/resultset/%s/resultdata?featureId=%s", RESULTDATA_SERVICE, resultSetId, featureId);
    }
}
