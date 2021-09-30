package eu.openanalytics.phaedra.calculationservice.service.status;

import eu.openanalytics.phaedra.calculationservice.dto.CalculationStatus;
import eu.openanalytics.phaedra.calculationservice.enumeration.CalculationStatusCode;
import eu.openanalytics.phaedra.calculationservice.model.CalculationContext;
import eu.openanalytics.phaedra.calculationservice.model.Feature;
import eu.openanalytics.phaedra.calculationservice.model.Protocol;
import eu.openanalytics.phaedra.calculationservice.model.Sequence;
import eu.openanalytics.phaedra.calculationservice.service.ModelMapper;
import eu.openanalytics.phaedra.calculationservice.service.protocol.ProtocolInfoCollector;
import eu.openanalytics.phaedra.platservice.client.PlateServiceClient;
import eu.openanalytics.phaedra.platservice.client.exception.PlateUnresolvableException;
import eu.openanalytics.phaedra.platservice.dto.WellDTO;
import eu.openanalytics.phaedra.protocolservice.client.exception.ProtocolUnresolvableException;
import eu.openanalytics.phaedra.resultdataservice.client.ResultDataServiceClient;
import eu.openanalytics.phaedra.resultdataservice.client.exception.ResultDataUnresolvableException;
import eu.openanalytics.phaedra.resultdataservice.client.exception.ResultFeatureStatUnresolvableException;
import eu.openanalytics.phaedra.resultdataservice.client.exception.ResultSetUnresolvableException;
import eu.openanalytics.phaedra.resultdataservice.dto.ResultDataDTO;
import eu.openanalytics.phaedra.resultdataservice.dto.ResultFeatureStatDTO;
import eu.openanalytics.phaedra.resultdataservice.enumeration.StatusCode;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

@Service
public class CalculationStatusService {

    private final ProtocolInfoCollector protocolInfoCollector;
    private final PlateServiceClient plateServiceClient;
    private final ResultDataServiceClient resultDataServiceClient;
    private final ModelMapper modelMapper;

    public CalculationStatusService(ProtocolInfoCollector protocolInfoCollector, PlateServiceClient plateServiceClient, ResultDataServiceClient resultDataServiceClient, ModelMapper modelMapper) {
        this.protocolInfoCollector = protocolInfoCollector;
        this.plateServiceClient = plateServiceClient;
        this.resultDataServiceClient = resultDataServiceClient;
        this.modelMapper = modelMapper;
    }

    public CalculationStatus.CalculationComplexityDTO numberOfSteps(CalculationContext cctx) {
        return numberOfSteps(cctx.protocol(), cctx.numWelltypes());
    }

    public CalculationStatus.CalculationComplexityDTO numberOfSteps(Protocol protocol, int numWelltypes) {
        // 3. determine number of steps
        int numberOfFeatures = 0;
        int numberOfFeatureStats = 0;
        int numberOfSequences = protocol.getSequences().size();

        for (var sequence : protocol.getSequences().entrySet()) {
            for (var feature : sequence.getValue().getFeatures()) {
                numberOfFeatures++;
                for (var featureStat : feature.getFeatureStats()) {
                    if (featureStat.isPlateStat()) {
                        numberOfFeatureStats++;
                    }
                    if (featureStat.isWelltypeStat()) {
                        numberOfFeatureStats += numWelltypes;
                    }
                }
            }
        }

        return new CalculationStatus.CalculationComplexityDTO(
                numberOfFeatures + numberOfFeatureStats,
                numberOfFeatures,
                numberOfFeatureStats,
                numberOfSequences
        );
    }

    public CalculationStatus getStatus(long resultSetId) throws ResultDataUnresolvableException, ResultSetUnresolvableException, ResultFeatureStatUnresolvableException, ProtocolUnresolvableException, PlateUnresolvableException {
        // 1. get resultSet
        final var resultSet = resultDataServiceClient.getResultSet(resultSetId);
        final var protocol = protocolInfoCollector.getProtocol(resultSet.getProtocolId());
        final var wellsSorted = plateServiceClient.getWellsOfPlateSorted(resultSet.getPlateId());

        // 2. determine number of unique wellTypes
        final var welltypesSorted = wellsSorted.stream().map(WellDTO::getWelltype).toList();
        final var uniqueWelltypes = new LinkedHashSet<>(welltypesSorted);
        final var numWelltypes = uniqueWelltypes.size();

        // 3. get required status
        final var complexity = numberOfSteps(protocol, numWelltypes);

        // 4. get ResultData
        final var resultData = resultDataServiceClient.getResultData(resultSetId);

        // 5. get ResultFeatureStat
        final var resultFeatures = resultDataServiceClient.getResultFeatureStat(resultSetId);

        final var resultDataByFeature = resultData.stream().collect(Collectors.toMap(ResultDataDTO::getFeatureId, it -> it));
        final var resultFeaturesByFeature = resultFeatures.stream().collect(Collectors.groupingBy(ResultFeatureStatDTO::getFeatureId, Collectors.toList()));

        // 6. calculate sequence status
        final var sequencesStatus = new HashMap<Integer, CalculationStatus.SequenceStatusDTO>();

        var defaultIfMissing = CalculationStatusCode.SCHEDULED;
        for (var seq = 0; seq < protocol.getSequences().size(); seq++) {
            Sequence sequence = protocol.getSequences().get(seq);
            var status = getSequenceStatus(sequence, resultDataByFeature, resultFeaturesByFeature, numWelltypes, defaultIfMissing);
            sequencesStatus.put(seq, status);
            if (status.getStatusCode() == CalculationStatusCode.FAILURE) {
                // if the current sequence was a failure -> all other sequences will be skipped
                defaultIfMissing = CalculationStatusCode.SKIPPED;
            }
        }

        return CalculationStatus.builder()
                .complexity(complexity)
                .sequences(sequencesStatus)
                .build();
    }

    public CalculationStatus.SequenceStatusDTO getSequenceStatus(Sequence sequence, Map<Long, ResultDataDTO> resultData, Map<Long, List<ResultFeatureStatDTO>> statData, int numWelltypes, CalculationStatusCode defaultIfMissing) {
        var statuses = new HashSet<CalculationStatusCode>(); // keep track of all statuses of this sequence, so that we can determine the final status of this sequence
        var featureStatuses = new HashMap<Long, CalculationStatus.FeatureStatusDTO>(); // keep track of the status of each feature in this sequence

        for (var feature : sequence.getFeatures()) {
            var statusCode = getFeatureStatus(resultData.get(feature.getId()), defaultIfMissing);
            HashMap<Long, CalculationStatusCode> statStatuses;
            if (statusCode == CalculationStatusCode.FAILURE) {
                // if the feature was a failure, the stats are always skipped
                statStatuses = getFeatureStatsStatus(feature, statData.get(feature.getId()), numWelltypes, CalculationStatusCode.SKIPPED);
            } else {
                statStatuses = getFeatureStatsStatus(feature, statData.get(feature.getId()), numWelltypes, defaultIfMissing);
            }
            statuses.add(statusCode);
            statuses.addAll(statStatuses.values());

            featureStatuses.put(
                    feature.getId(),
                    new CalculationStatus.FeatureStatusDTO(
                            statusCode,
                            getAggregatedStatusCode(statStatuses.values()),
                            statStatuses
                    ));
        }

        return new CalculationStatus.SequenceStatusDTO(getAggregatedStatusCode(statuses), featureStatuses);
    }

    public CalculationStatusCode getFeatureStatus(ResultDataDTO resultDataDTO, CalculationStatusCode defaultIfMissing) {
        if (resultDataDTO == null) {
            // no result found for this Feature -> return default
            return defaultIfMissing;
        }
        return modelMapper.map(resultDataDTO.getStatusCode());
    }

    public HashMap<Long, CalculationStatusCode> getFeatureStatsStatus(Feature feature, List<ResultFeatureStatDTO> statData, int numWelltypes, CalculationStatusCode defaultIfMissing) {
        var res = new HashMap<Long, CalculationStatusCode>();

        var statResultsByStatId = Objects.requireNonNullElse(statData, new ArrayList<ResultFeatureStatDTO>()).stream().collect(Collectors.groupingBy(ResultFeatureStatDTO::getFeatureStatId, Collectors.toList()));
        for (var featureStat : feature.getFeatureStats()) {
            var requiredAmount = 0;
            if (featureStat.isPlateStat()) {
                requiredAmount++;
            }
            if (featureStat.isWelltypeStat()) {
                requiredAmount += numWelltypes;
            }

            var statResults = statResultsByStatId.get(featureStat.getId());

            if (statResults == null || statResults.size() != requiredAmount) {
                // did not find all Results -> return default
                res.put(featureStat.getId(), defaultIfMissing);
            } else {
                if (statResults.stream().allMatch(it -> it.getStatusCode() == StatusCode.SUCCESS)) {
                    res.put(featureStat.getId(), CalculationStatusCode.SUCCESS);
                } else {
                    res.put(featureStat.getId(), CalculationStatusCode.FAILURE);
                }
            }
        }
        return res;
    }

    @SafeVarargs
    public final CalculationStatusCode getAggregatedStatusCode(Collection<CalculationStatusCode>... lists) {
        var statuses = Arrays.stream(lists).flatMap(Collection::stream).collect(Collectors.toSet());

        if (statuses.contains(CalculationStatusCode.SCHEDULED)) {
            return CalculationStatusCode.SCHEDULED;
        }

        if (statuses.contains(CalculationStatusCode.FAILURE)) {
            return CalculationStatusCode.FAILURE;
        }

        if (statuses.contains(CalculationStatusCode.SKIPPED)) {
            return CalculationStatusCode.SKIPPED;
        }

        return CalculationStatusCode.SUCCESS;
    }

}
