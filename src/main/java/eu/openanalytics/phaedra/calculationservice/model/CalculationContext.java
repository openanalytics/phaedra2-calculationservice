package eu.openanalytics.phaedra.calculationservice.model;

import eu.openanalytics.phaedra.calculationservice.service.protocol.ErrorCollector;
import eu.openanalytics.phaedra.platservice.dto.PlateDTO;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;

public record CalculationContext(
        PlateDTO plate,
        Protocol protocol,
        Long resultSetId,
        Long measId,
        ErrorCollector errorCollector,
        List<String> welltypesSorted,
        LinkedHashSet<String> uniqueWelltypes,
        int numWelltypes,
        ConcurrentHashMap<Feature, Future<Boolean>> computedStatsForFeature) {

}
