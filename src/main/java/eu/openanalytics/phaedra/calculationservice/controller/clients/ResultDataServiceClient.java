package eu.openanalytics.phaedra.calculationservice.controller.clients;

import eu.openanalytics.phaedra.calculationservice.dto.external.ResultDataDTO;
import eu.openanalytics.phaedra.calculationservice.dto.external.ResultSetDTO;
import eu.openanalytics.phaedra.calculationservice.scriptengineclient.model.ResponseStatusCode;

public interface ResultDataServiceClient {

    ResultSetDTO createResultDataSet(long protocolId, long plateId, long measId) throws ResultSetUnresolvableException;

    ResultSetDTO completeResultDataSet(long resultSetId, String outcome) throws ResultSetUnresolvableException;

    ResultDataDTO addResultData(long resultSetId, long featureId, float[] values, ResponseStatusCode statusCode, String statusMessage, Integer exitCode) throws ResultDataUnresolvableException;

    ResultDataDTO getResultData(long resultSetId, long featureId) throws ResultDataUnresolvableException;

}
