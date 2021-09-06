package eu.openanalytics.phaedra.calculationservice;

import eu.openanalytics.phaedra.resultdataservice.client.ResultDataServiceClient;
import eu.openanalytics.phaedra.resultdataservice.client.exception.ResultDataUnresolvableException;
import eu.openanalytics.phaedra.resultdataservice.dto.ErrorDTO;
import eu.openanalytics.phaedra.resultdataservice.dto.ResultDataDTO;
import eu.openanalytics.phaedra.resultdataservice.dto.ResultSetDTO;
import eu.openanalytics.phaedra.resultdataservice.enumeration.StatusCode;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class InMemoryResultDataServiceClient implements ResultDataServiceClient {

    private final List<ResultSetDTO> resultSets = new ArrayList<>();
    private final Map<Long, List<ResultDataDTO>> resultData = new HashMap<>();

    @Override
    public synchronized ResultSetDTO createResultDataSet(long protocolId, long plateId, long measId) {
        var newId = (long) resultSets.size();
        var resultSet = new ResultSetDTO(newId, protocolId, plateId, measId, LocalDateTime.now(), null, null, null, null);
        resultSets.add(resultSet);
        resultData.put(newId, new ArrayList<>());
        return resultSet;
    }

    @Override
    public ResultSetDTO completeResultDataSet(long resultSetId, String outcome, List<ErrorDTO> errors, String errorsText) {
        var resultSet = resultSets.get((int) resultSetId)
                .withOutcome(outcome).withErrors(errors).withErrorsText(errorsText);
        resultSets.set((int) resultSetId, resultSet);
        return resultSet;
    }

    @Override
    public ResultDataDTO addResultData(long resultSetId, long featureId, float[] values, StatusCode statusCode, String statusMessage, Integer exitCode) {
        var newId = resultData.get(resultSetId).size();
        var res = new ResultDataDTO((long) newId, resultSetId, featureId, values, statusCode, statusMessage, exitCode, LocalDateTime.now());
        resultData.get(resultSetId).add(res);
        return res;
    }

    @Override
    public ResultDataDTO getResultData(long resultId, long featureId) throws ResultDataUnresolvableException {
        var res = resultData.get(resultId).stream().filter((x) -> x.getFeatureId().equals(featureId)).findFirst();
        if (res.isEmpty()) {
            throw new ResultDataUnresolvableException("ResultData not found");
        }
        return res.get();
    }

}
