package eu.openanalytics.phaedra.calculationservice.model;

import eu.openanalytics.phaedra.calculationservice.service.protocol.ErrorCollector;
import eu.openanalytics.phaedra.plateservice.dto.PlateDTO;
import eu.openanalytics.phaedra.plateservice.dto.WellDTO;
import eu.openanalytics.phaedra.plateservice.dto.WellSubstanceDTO;
import eu.openanalytics.phaedra.protocolservice.dto.FeatureDTO;
import lombok.*;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

@Data
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@Setter(AccessLevel.PRIVATE)
public class CurveFittingContext {
    @NonNull
    PlateDTO plate;

    @NonNull
    List<WellDTO> wells;

    @NonNull
    List<WellSubstanceDTO> wellSubstances;

    @NonNull
    List<String> uniqueSubstances;

    @NonNull
    List<FeatureDTO> curveFeatures;

    @NonNull
    Long resultSetId;

    public static CurveFittingContext newInstance(PlateDTO plate,
                                                  List<WellDTO> wells,
                                                 List<WellSubstanceDTO> wellSubstances,
                                                 List<String> uniqueSubstances,
                                                 List<FeatureDTO> curveFeatures,
                                                 Long resultSetId) {
        var res = new CurveFittingContext(plate, wells, wellSubstances, uniqueSubstances, curveFeatures, resultSetId);
        return res;
    }
}
