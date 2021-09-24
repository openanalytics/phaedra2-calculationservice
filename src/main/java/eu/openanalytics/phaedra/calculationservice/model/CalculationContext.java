package eu.openanalytics.phaedra.calculationservice.model;

import eu.openanalytics.phaedra.calculationservice.service.protocol.ErrorCollector;
import eu.openanalytics.phaedra.platservice.dto.PlateDTO;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;

public record CalculationContext(
        PlateDTO plate,
        Protocol protocol,
        Long resultSetId,
        Long measId,
        ErrorCollector errorCollector,
        ConcurrentHashMap<Feature, Future<Boolean>> computedStatsForFeature) {

}
