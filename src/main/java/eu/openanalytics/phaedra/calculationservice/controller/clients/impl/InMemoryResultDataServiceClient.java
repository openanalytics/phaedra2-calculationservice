package eu.openanalytics.phaedra.calculationservice.controller.clients.impl;

import eu.openanalytics.phaedra.calculationservice.controller.clients.ResultDataServiceClient;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Component
public class InMemoryResultDataServiceClient implements ResultDataServiceClient {

    private final List<ResultDataSet> resultDataSet = new ArrayList<>();

    @Override
    public synchronized long createResultDataSet(long protocolId, long plateId, long measId) {
        resultDataSet.add(new ResultDataSet(protocolId, plateId, measId, null, new HashMap<>()));
        return resultDataSet.size() - 1;
    }

    @Override
    public synchronized void finishResultDataSet(long resultId, String outcome) {
        resultDataSet.get((int) resultId).outcome = outcome;
    }

    @Override
    public synchronized void addResultData(long resultId, long featureId, float[] result, int statusCode, String statusMessage) {
        System.out.printf("Add resultData %s, %s\n", resultId, featureId);
        System.out.println(Arrays.toString(result));
        resultDataSet.get((int) resultId).resultDataSets.put(featureId, new ResultData(featureId, result, statusCode, statusMessage));
    }

    @Override
    public float[] getResultData(long resultId, long featureId) {
        return resultDataSet.get((int) resultId).getResultDataSets().get(featureId).resultValues;
    }

    @Data
    @AllArgsConstructor
    public static class ResultDataSet {
        Long protocolId;
        Long plateId;
        Long measId;
        String outcome;
        Map<Long, ResultData> resultDataSets;
    }

    @Data
    @AllArgsConstructor
    public static class ResultData {
        Long featureId;
        float[] resultValues;
        int statusCode;
        String statusMessage;
    }


}
