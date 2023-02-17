package eu.openanalytics.phaedra.calculationservice.service;

import eu.openanalytics.curvedataservice.dto.CurveDTO;
import eu.openanalytics.phaedra.calculationservice.config.KafkaConfig;
import eu.openanalytics.phaedra.calculationservice.dto.CurveFittingRequestDTO;
import eu.openanalytics.phaedra.plateservice.dto.PlateCalculationStatusDTO;
import eu.openanalytics.phaedra.resultdataservice.dto.ResultDataDTO;
import eu.openanalytics.phaedra.resultdataservice.dto.ResultFeatureStatDTO;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.ArrayList;

@Service
public class KafkaProducerService {
    private final KafkaTemplate<String, Object> kafkaTemplate;

    public KafkaProducerService(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendPlateCalculationStatus(PlateCalculationStatusDTO plateCalculationStatusDTO) {
        kafkaTemplate.send(KafkaConfig.PLATE_TOPIC, KafkaConfig.UPDATE_PLATE_STATUS_EVENT, plateCalculationStatusDTO);
    }

    public void sendCurveData(CurveDTO curveDTO) {
        kafkaTemplate.send(KafkaConfig.CURVEDATA_TOPIC, KafkaConfig.SAVE_CURVE_EVENT, curveDTO);
    }

    public void initiateCurveFitting(CurveFittingRequestDTO curveFitRequest) {
        kafkaTemplate.send(KafkaConfig.CALCULATIONS_TOPIC, KafkaConfig.CURVE_FIT_EVENT, curveFitRequest);
    }

    public void sendResultData(ResultDataDTO resultData) {
        kafkaTemplate.send(KafkaConfig.RESULTDATA_TOPIC, KafkaConfig.SAVE_FEATURE_RESULTDATA_EVENT, resultData);
    }

    public void sendResultFeatureStats(Long resultSetId, ArrayList<ResultFeatureStatDTO> resultFeatureStats) {
        for (ResultFeatureStatDTO resultFeatureStatDTO: resultFeatureStats) {
            kafkaTemplate.send(KafkaConfig.RESULTDATA_TOPIC, KafkaConfig.SAVE_FEATURE_STATS_EVENT, resultFeatureStatDTO.withResultSetId(resultSetId));
        }
    }
}
