package eu.openanalytics.phaedra.calculationservice.controller.clients;

import eu.openanalytics.phaedra.model.v2.dto.ErrorDTO;
import eu.openanalytics.phaedra.model.v2.dto.ResultDataDTO;
import eu.openanalytics.phaedra.model.v2.dto.ResultSetDTO;
import eu.openanalytics.phaedra.model.v2.enumeration.ResponseStatusCode;

import java.util.List;

public interface ResultDataServiceClient {

    ResultSetDTO createResultDataSet(long protocolId, long plateId, long measId) throws ResultSetUnresolvableException;

    ResultSetDTO completeResultDataSet(long resultSetId, String outcome, List<ErrorDTO> errors, String errorsText) throws ResultSetUnresolvableException;

    ResultDataDTO addResultData(long resultSetId, long featureId, float[] values, ResponseStatusCode statusCode, String statusMessage, Integer exitCode) throws ResultDataUnresolvableException;

    ResultDataDTO getResultData(long resultSetId, long featureId) throws ResultDataUnresolvableException;

}
