package eu.openanalytics.phaedra.calculationservice.model;

import eu.openanalytics.phaedra.calculationservice.service.protocol.ErrorCollector;
import eu.openanalytics.phaedra.platservice.dto.PlateDTO;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Setter;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;

@Data
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@Setter(AccessLevel.PRIVATE)
public class CalculationContext {

    PlateDTO plate;
    Protocol protocol;
    Long resultSetId;
    Long measId;
    ErrorCollector errorCollector;
    List<String> welltypesSorted;
    LinkedHashSet<String> uniqueWelltypes;
    int numWelltypes;
    ConcurrentHashMap<Feature, Future<Boolean>> computedStatsForFeature;

    public static CalculationContext newInstance(PlateDTO plate,
                                          Protocol protocol,
                                          Long resultSetId,
                                          Long measId,
                                          List<String> welltypesSorted,
                                          LinkedHashSet<String> uniqueWelltypes
    ) {
        var res = new CalculationContext(plate, protocol, resultSetId, measId,
                null, welltypesSorted, uniqueWelltypes, uniqueWelltypes.size(),
                new ConcurrentHashMap<>());
        res.errorCollector = new ErrorCollector(res);
        return res;
    }

}
