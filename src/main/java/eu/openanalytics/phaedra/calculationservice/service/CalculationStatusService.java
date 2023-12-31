/**
 * Phaedra II
 *
 * Copyright (C) 2016-2023 Open Analytics
 *
 * ===========================================================================
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the Apache License as published by
 * The Apache Software Foundation, either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Apache License for more details.
 *
 * You should have received a copy of the Apache License
 * along with this program.  If not, see <http://www.apache.org/licenses/>
 */
package eu.openanalytics.phaedra.calculationservice.service;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import org.springframework.stereotype.Service;

import eu.openanalytics.phaedra.calculationservice.dto.CalculationStatus;
import eu.openanalytics.phaedra.calculationservice.enumeration.CalculationStatusCode;
import eu.openanalytics.phaedra.calculationservice.model.CalculationContext;
import eu.openanalytics.phaedra.calculationservice.model.ModelMapper;
import eu.openanalytics.phaedra.calculationservice.service.protocol.ProtocolDataCollector;
import eu.openanalytics.phaedra.plateservice.client.PlateServiceClient;
import eu.openanalytics.phaedra.plateservice.client.exception.PlateUnresolvableException;
import eu.openanalytics.phaedra.plateservice.dto.WellDTO;
import eu.openanalytics.phaedra.protocolservice.client.exception.ProtocolUnresolvableException;
import eu.openanalytics.phaedra.protocolservice.dto.FeatureDTO;
import eu.openanalytics.phaedra.protocolservice.dto.FeatureStatDTO;
import eu.openanalytics.phaedra.resultdataservice.client.ResultDataServiceClient;
import eu.openanalytics.phaedra.resultdataservice.client.exception.ResultDataUnresolvableException;
import eu.openanalytics.phaedra.resultdataservice.client.exception.ResultFeatureStatUnresolvableException;
import eu.openanalytics.phaedra.resultdataservice.client.exception.ResultSetUnresolvableException;
import eu.openanalytics.phaedra.resultdataservice.dto.ResultDataDTO;
import eu.openanalytics.phaedra.resultdataservice.dto.ResultFeatureStatDTO;
import eu.openanalytics.phaedra.resultdataservice.enumeration.StatusCode;

@Service
public class CalculationStatusService {

    private final ProtocolDataCollector protocolInfoCollector;
    private final PlateServiceClient plateServiceClient;
    private final ResultDataServiceClient resultDataServiceClient;
    private final ModelMapper modelMapper;

    private static final CalculationStatus.StatusDescription DESCR_SKIPPED_PREV_SEQ_FAILED =
            CalculationStatus.StatusDescription.builder()
                    .statusCode(CalculationStatusCode.SKIPPED)
                    .description("Skipped because previous sequence failed.")
                    .build();

    private static final CalculationStatus.StatusDescription DESCR_SKIPPED_FEATURE_FAILED =
            CalculationStatus.StatusDescription.builder()
                    .statusCode(CalculationStatusCode.SKIPPED)
                    .description("Skipped because calculating the corresponding feature failed.")
                    .build();

    private static final CalculationStatus.StatusDescription DESCR_SCHEDULED =
            CalculationStatus.StatusDescription.builder()
                    .statusCode(CalculationStatusCode.SCHEDULED)
                    .build();

    private static final CalculationStatus.StatusDescription DESCR_FEATURE_FAILED =
            CalculationStatus.StatusDescription.builder()
                    .statusCode(CalculationStatusCode.FAILURE)
                    .description("Sequence marked as failed because at least one feature failed.")
                    .build();

    private static final CalculationStatus.StatusDescription DESCR_STAT_FAILED =
            CalculationStatus.StatusDescription.builder()
                    .statusCode(CalculationStatusCode.FAILURE)
                    .description("Sequence marked as failed because at least one featureStat failed (next sequence will still be calculated).")
                    .build();

    public CalculationStatusService(ProtocolDataCollector protocolInfoCollector, PlateServiceClient plateServiceClient, ResultDataServiceClient resultDataServiceClient, ModelMapper modelMapper) {
        this.protocolInfoCollector = protocolInfoCollector;
        this.plateServiceClient = plateServiceClient;
        this.resultDataServiceClient = resultDataServiceClient;
        this.modelMapper = modelMapper;
    }

    /**
     * Gets the status of a calculation using the id of the {@see ResultSet} of this calculation.
     * @param resultSetId the resultSetId corresponding to this calculation.
     * @return the status of the calculation
     * @throws ResultDataUnresolvableException
     * @throws ResultSetUnresolvableException
     * @throws ResultFeatureStatUnresolvableException
     * @throws ProtocolUnresolvableException
     * @throws PlateUnresolvableException
     */
    public CalculationStatus getStatus(long resultSetId) throws ResultDataUnresolvableException, ResultSetUnresolvableException, ResultFeatureStatUnresolvableException, ProtocolUnresolvableException, PlateUnresolvableException {
    	
        var resultSet = resultDataServiceClient.getResultSet(resultSetId);
        var protocolData = protocolInfoCollector.getProtocolData(resultSet.getProtocolId());
        var wells = plateServiceClient.getWells(resultSet.getPlateId());
        CalculationContext ctx = CalculationContext.newInstance(protocolData, null, wells, resultSetId, resultSet.getMeasId());

        var resultData = resultDataServiceClient.getResultData(resultSetId);
        var resultDataByFeature = resultData.stream().collect(Collectors.toMap(ResultDataDTO::getFeatureId, it -> it));

        var resultFeatures = resultDataServiceClient.getResultFeatureStat(resultSetId);
        var resultFeaturesByFeature = resultFeatures.stream().collect(Collectors.groupingBy(ResultFeatureStatDTO::getFeatureId, Collectors.toList()));

        var sequencesStatus = new HashMap<Integer, CalculationStatus.SequenceStatusDTO>();

        var defaultIfMissing = DESCR_SCHEDULED;
        for (Integer seq: protocolData.sequences.keySet().stream().sorted().toList()) {
            var status = getSequenceStatus(seq, ctx, resultDataByFeature, resultFeaturesByFeature, defaultIfMissing);
            sequencesStatus.put(seq, status);
            if (status.getStatus().getStatusCode() == CalculationStatusCode.FAILURE) {
                // if the current sequence was a failure -> all other sequences will be skipped
                defaultIfMissing = DESCR_SKIPPED_PREV_SEQ_FAILED;
            }
        }

        return CalculationStatus.builder()
                .complexity(getComplexity(ctx))
                .sequences(sequencesStatus)
                .statusCode(resultSet.getOutcome())
                .errors(resultSet.getErrors())
                .build();
    }

    /**
     * Get the status of a single {@see Sequence}.
     * @param sequence the sequence to calculate the status for
     * @param resultData the results of the Features of this calculation
     * @param statData the results of the FeatureStats of this calculation
     * @param numWelltypes the number of wellTypes used in this calculation
     * @param defaultIfMissing the value to use when no result is found for the given Feature or FeatureStat
     * @return the status of this sequence
     */
    public CalculationStatus.SequenceStatusDTO getSequenceStatus(Integer sequence, CalculationContext ctx, Map<Long, ResultDataDTO> resultData, Map<Long, List<ResultFeatureStatDTO>> statData, CalculationStatus.StatusDescription defaultIfMissing) {

    	var featureStatuses = new HashMap<Long, CalculationStatus.FeatureStatusDTO>();
        var featureStatStatuses = new HashSet<CalculationStatus.StatusDescription>();

        var sequenceFeatures = ctx.getProtocolData().protocol.getFeatures().stream()
        		.filter(f -> f.getSequence() == sequence).toList();
        
        for (var feature : sequenceFeatures) {
            var statusCode = getFeatureStatus(resultData.get(feature.getId()), defaultIfMissing);
            HashMap<Long, CalculationStatus.StatusDescription> statStatuses;
            if (statusCode.getStatusCode() == CalculationStatusCode.FAILURE) {
                // if the feature was a failure, the stats are always skipped
                statStatuses = getFeatureStatsStatus(feature, ctx, statData.get(feature.getId()), DESCR_SKIPPED_FEATURE_FAILED);
            } else {
                statStatuses = getFeatureStatsStatus(feature, ctx, statData.get(feature.getId()), defaultIfMissing);
            }
            featureStatStatuses.addAll(statStatuses.values());
            featureStatuses.put(
                    feature.getId(),
                    new CalculationStatus.FeatureStatusDTO(
                            statusCode,
                            new CalculationStatus.StatusDescription(getAggregatedStatusCode(statStatuses.values())),
                            statStatuses
                    ));
        }

        return new CalculationStatus.SequenceStatusDTO(getAggregatedStatusCode(featureStatuses.values(), featureStatStatuses), featureStatuses);
    }

    /**
     * Calculates the {@see CalculationStatus.StatusDescription} for a single feature based on the result for this feature.
     * @param resultDataDTO the result data of this feature, may be null when no such result exists
     * @param defaultIfMissing the value to return when there is no result data for this feature
     * @return the status of this feature
     */
    public CalculationStatus.StatusDescription getFeatureStatus(ResultDataDTO resultDataDTO, CalculationStatus.StatusDescription defaultIfMissing) {
        if (resultDataDTO == null) {
            // no result found for this Feature -> return default
            return defaultIfMissing;
        }
        var statusCode = modelMapper.map(resultDataDTO.getStatusCode());
        if (statusCode == CalculationStatusCode.FAILURE) {
            // attach the StatusMessage when this calculation failed.
            return CalculationStatus.StatusDescription.builder()
                    .statusCode(statusCode)
                    .statusMessage(resultDataDTO.getStatusMessage())
                    .build();
        } else {
            return new CalculationStatus.StatusDescription(statusCode);
        }
    }

    /**
     * Calculates the {@see CalculationStatus.StatusDescription} for each FeatureStat of the given Feature.
     * @param feature the feature to calculate the status for
     * @param statData the result data of the featureStats for this feature
     * @param numWelltypes the number of wellTypes used in this calculation
     * @param defaultIfMissing the value to return when there is no result data for a featureStat of this feature
     * @return the status for each FeatureStat in this feature
     */
    public HashMap<Long, CalculationStatus.StatusDescription> getFeatureStatsStatus(FeatureDTO feature, CalculationContext ctx, List<ResultFeatureStatDTO> statData, CalculationStatus.StatusDescription defaultIfMissing) {
        var res = new HashMap<Long, CalculationStatus.StatusDescription>();

        var featureStats = ctx.getProtocolData().featureStats.get(feature.getId());
        var statResultsByStatId = Objects.requireNonNullElse(statData, new ArrayList<ResultFeatureStatDTO>()).stream().collect(Collectors.groupingBy(ResultFeatureStatDTO::getFeatureStatId, Collectors.toList()));
        int numberOfWellTypes = (int) ctx.getWells().stream().map(WellDTO::getWellType).distinct().count();
        
        for (var featureStat : featureStats) {
            var numOfExpectedResults = getNumOfExpectedFeatureStats(featureStat, numberOfWellTypes);
            var statResults = statResultsByStatId.get(featureStat.getId());

            if (statResults == null || statResults.size() != numOfExpectedResults) {
                // did not find all Results -> return default
                res.put(featureStat.getId(), defaultIfMissing);
            } else {
                // found a result for each featureStat
                // note: by design of the CalculationService it's guaranteed that each result has the same StatusCode
                // note: the status of a FeatureStat can only be SUCCESS or FAILURE
                var statusCode = statResults.get(0).getStatusCode();
                if (statusCode == StatusCode.SUCCESS) {
                    res.put(featureStat.getId(), new CalculationStatus.StatusDescription(CalculationStatusCode.SUCCESS));
                } else if (statusCode == StatusCode.FAILURE) {
                    var statusMessage = statResults.stream().map(ResultFeatureStatDTO::getStatusMessage)
                            .distinct() // the results probably have the same status message -> call distinct()
                            .collect(Collectors.joining("\n"));
                    res.put(featureStat.getId(), CalculationStatus.StatusDescription.builder()
                            .statusMessage(statusMessage)
                            .statusCode(CalculationStatusCode.FAILURE)
                                .build());
                }
            }
        }
        return res;
    }

    private CalculationStatus.CalculationComplexityDTO getComplexity(CalculationContext ctx) {
    	
        int numberOfFeatures = ctx.getProtocolData().protocol.getFeatures().size();
        int numberOfFeatureStats = (int) ctx.getProtocolData().featureStats.values().stream().flatMap(l -> l.stream()).count();
        int numberOfWellTypes = (int) ctx.getWells().stream().map(WellDTO::getWellType).distinct().count();
        int numberOfFeatureStatResults =
        		ctx.getProtocolData().featureStats.values().stream().flatMap(l -> l.stream())
        		.mapToInt(fs -> getNumOfExpectedFeatureStats(fs, numberOfWellTypes))
        		.sum();
        int numberOfSequences = ctx.getProtocolData().sequences.size();

        return new CalculationStatus.CalculationComplexityDTO(
                numberOfFeatures + numberOfFeatureStats,
                numberOfFeatures,
                numberOfFeatureStats,
                numberOfFeatureStatResults,
                numberOfSequences
        );
    }
    
    /**
     * Calculates the StatusDescription for a sequence based on the statuses of the feature and featueStats of the sequence.
     * @param featureStatuses the statuses of the features in this sequence
     * @param statStatuses the status of the FeatureStats in this sequence
     * @return the aggregated status of the sequence
     */
    private CalculationStatus.StatusDescription getAggregatedStatusCode(Collection<CalculationStatus.FeatureStatusDTO> featureStatuses, Collection<CalculationStatus.StatusDescription> statStatuses) {
        // check whether any feature failed
        var aggregatedFeatureStatus = getAggregatedStatusCode(featureStatuses.stream().map(CalculationStatus.FeatureStatusDTO::getStatus).collect(Collectors.toSet()));
        if (aggregatedFeatureStatus == CalculationStatusCode.FAILURE) {
            return DESCR_FEATURE_FAILED;
        }

        // check whether any FeatureStat failed
        var aggregatedFeatureStatStatus = getAggregatedStatusCode(statStatuses);
        if (aggregatedFeatureStatStatus == CalculationStatusCode.FAILURE) {
            return DESCR_STAT_FAILED;
        }

        return new CalculationStatus.StatusDescription(getAggregatedStatusCode(List.of(aggregatedFeatureStatus, aggregatedFeatureStatStatus)));
    }

    /***
     * Calculate the aggregated status from the given statuses.
     */
    @SafeVarargs
    private CalculationStatusCode getAggregatedStatusCode(Collection<CalculationStatus.StatusDescription>... lists) {
        var statuses = Arrays.stream(lists).flatMap(Collection::stream)
                .map(CalculationStatus.StatusDescription::getStatusCode)
                .collect(Collectors.toSet());
        return getAggregatedStatusCode(statuses);
    }

    /**
     * Calculate the aggregated status from the given statuses.
     */
    private CalculationStatusCode getAggregatedStatusCode(Collection<CalculationStatusCode> statuses) {
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

    /**
     * Calculate the number of expected results for the given {@see FeatureStat}.
     * @param featureStat the featurestat
     * @param numWelltypes the number of welltypes used in the calculation
     * @return the number of expected results
     */
    private int getNumOfExpectedFeatureStats(FeatureStatDTO featureStat, int numWelltypes) {
        var requiredAmount = 0;
        if (featureStat.getPlateStat()) {
            requiredAmount++;
        }
        if (featureStat.getWelltypeStat()) {
            requiredAmount += numWelltypes;
        }
        return requiredAmount;
    }

}
