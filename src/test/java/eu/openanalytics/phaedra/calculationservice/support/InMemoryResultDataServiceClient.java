package eu.openanalytics.phaedra.calculationservice.support;

import eu.openanalytics.phaedra.resultdataservice.client.ResultDataServiceClient;
import eu.openanalytics.phaedra.resultdataservice.client.exception.ResultDataUnresolvableException;
import eu.openanalytics.phaedra.resultdataservice.client.exception.ResultFeatureStatUnresolvableException;
import eu.openanalytics.phaedra.resultdataservice.client.exception.ResultSetUnresolvableException;
import eu.openanalytics.phaedra.resultdataservice.dto.ErrorDTO;
import eu.openanalytics.phaedra.resultdataservice.dto.ResultDataDTO;
import eu.openanalytics.phaedra.resultdataservice.dto.ResultFeatureStatDTO;
import eu.openanalytics.phaedra.resultdataservice.dto.ResultSetDTO;
import eu.openanalytics.phaedra.resultdataservice.enumeration.StatusCode;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class InMemoryResultDataServiceClient implements ResultDataServiceClient {

    private final List<ResultSetDTO> resultSets = new ArrayList<>();
    private final Map<Long, List<ResultDataDTO>> resultData = new HashMap<>();
    private final List<ResultFeatureStatDTO> featureStats = new ArrayList<>();

    @Override
    public synchronized ResultSetDTO createResultDataSet(long protocolId, long plateId, long measId) {
        var newId = (long) resultSets.size();
        var resultSet = new ResultSetDTO(newId, protocolId, plateId, measId, LocalDateTime.now(), null, null, null, null);
        resultSets.add(resultSet);
        resultData.put(newId, new ArrayList<>());
        return resultSet;
    }

    @Override
    public synchronized ResultSetDTO completeResultDataSet(long resultSetId, StatusCode outcome, List<ErrorDTO> errors, String errorsText) {
        var resultSet = resultSets.get((int) resultSetId)
                .withOutcome(outcome).withErrors(errors).withErrorsText(errorsText);
        resultSets.set((int) resultSetId, resultSet);
        return resultSet;
    }

    @Override
    public synchronized ResultDataDTO addResultData(long resultSetId, long featureId, float[] values, StatusCode statusCode, String statusMessage, Integer exitCode) {
        var newId = resultData.get(resultSetId).size();
        var res = new ResultDataDTO((long) newId, resultSetId, featureId, values, statusCode, statusMessage, exitCode, LocalDateTime.now());
        resultData.get(resultSetId).add(res);
        return res;
    }

    @Override
    public synchronized ResultDataDTO getResultData(long resultId, long featureId) throws ResultDataUnresolvableException {
        var res = resultData.get(resultId).stream().filter((x) -> x.getFeatureId().equals(featureId)).findFirst();
        if (res.isEmpty()) {
            throw new ResultDataUnresolvableException("ResultData not found");
        }
        return res.get();
    }

    @Override
    public synchronized ResultFeatureStatDTO createResultFeatureStat(long resultSetId, long featureId, long featureStatId, Optional<Float> value, String statisticName, String welltype, StatusCode statusCode, String statusMessage, Integer exitCode) {
        var newId = (long) featureStats.size();
        var res = new ResultFeatureStatDTO(newId, resultSetId, featureId, featureStatId, value.orElse(null), statisticName, welltype, statusCode, statusMessage, exitCode, LocalDateTime.now());
        featureStats.add(res);
        return res;
    }

    @Override
    public List<ResultFeatureStatDTO> createResultFeatureStats(long resultSetId, List<ResultFeatureStatDTO> resultFeatureStats) throws ResultFeatureStatUnresolvableException {
        var res = new ArrayList<ResultFeatureStatDTO>();
        for (var resultFeatureStat : resultFeatureStats) {
            var newId = (long) featureStats.size();
            resultFeatureStat = resultFeatureStat.toBuilder().id(newId).resultSetId(resultSetId).build();
            featureStats.add(resultFeatureStat);
            res.add(resultFeatureStat);
        }
        return res;
    }

    @Override
    public synchronized ResultFeatureStatDTO getResultFeatureStat(long resultSetId, long resultFeatureStatId) throws ResultFeatureStatUnresolvableException {
        var res = featureStats.get((int) resultFeatureStatId);
        if (res == null || res.getResultSetId() != resultSetId) {
            throw new ResultFeatureStatUnresolvableException("ResultFeatureStat not found");
        }
        return res;
    }

    @Override
    public ResultSetDTO getResultSet(long resultSetId) throws ResultSetUnresolvableException {
        return null;
    }

    @Override
    public List<ResultSetDTO> getResultSet(StatusCode outcome) throws ResultSetUnresolvableException {
        return null;
    }

    @Override
    public List<ResultDataDTO> getResultData(long resultSetId) throws ResultDataUnresolvableException {
        return null;
    }

    @Override
    public List<ResultFeatureStatDTO> getResultFeatureStat(long resultSetId) {
        return null;
    }

}
