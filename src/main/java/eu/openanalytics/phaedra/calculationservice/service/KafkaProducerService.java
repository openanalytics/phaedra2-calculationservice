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
