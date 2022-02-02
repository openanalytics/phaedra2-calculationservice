package eu.openanalytics.phaedra.calculationservice.model;

import eu.openanalytics.phaedra.calculationservice.service.protocol.ErrorCollector;
import eu.openanalytics.phaedra.plateservice.dto.PlateDTO;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NonNull;
import lombok.Setter;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;

@Data
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@Setter(AccessLevel.PRIVATE)
public class CalculationContext {

    @NonNull
    PlateDTO plate;

    @NonNull
    Protocol protocol;

    @NonNull
    Long resultSetId;

    @NonNull
    Long measId;

    ErrorCollector errorCollector;

    @NonNull
    List<String> welltypesSorted;

    @NonNull
    LinkedHashSet<String> uniqueWelltypes;

    int numWelltypes;

    @NonNull
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
