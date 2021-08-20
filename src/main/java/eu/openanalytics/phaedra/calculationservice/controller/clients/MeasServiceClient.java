package eu.openanalytics.phaedra.calculationservice.controller.clients;

public interface MeasServiceClient {

    float[] getWellData(long measId, String columnName) throws MeasUnresolvableException;

}
