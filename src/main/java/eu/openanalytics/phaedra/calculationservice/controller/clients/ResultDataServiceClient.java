package eu.openanalytics.phaedra.calculationservice.controller.clients;

public interface ResultDataServiceClient {

    long createResultDataSet(long protocolId, long plateId, long measId);

    void finishResultDataSet(long resultId, String outcome);

    void addResultData(long resultId, long featureId, float[] result, int statusCode, String statusMessage);

    float[] getResultData(long resultId, long featureId);

}
