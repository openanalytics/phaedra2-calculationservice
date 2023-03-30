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

import java.util.List;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import eu.openanalytics.curvedataservice.dto.CurveDTO;
import eu.openanalytics.phaedra.calculationservice.config.KafkaConfig;
import eu.openanalytics.phaedra.calculationservice.dto.CurveFittingRequestDTO;
import eu.openanalytics.phaedra.plateservice.dto.PlateCalculationStatusDTO;
import eu.openanalytics.phaedra.resultdataservice.dto.ResultDataDTO;
import eu.openanalytics.phaedra.resultdataservice.dto.ResultFeatureStatDTO;
import eu.openanalytics.phaedra.scriptengine.dto.ScriptExecutionInputDTO;

@Service
public class KafkaProducerService {
	
    private final KafkaTemplate<String, Object> kafkaTemplate;

    public KafkaProducerService(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendPlateCalculationStatus(PlateCalculationStatusDTO plateCalculationStatusDTO) {
        kafkaTemplate.send(KafkaConfig.TOPIC_PLATES, KafkaConfig.EVENT_UPDATE_PLATE_STATUS, plateCalculationStatusDTO);
    }

    public void sendCurveData(CurveDTO curveDTO) {
        kafkaTemplate.send(KafkaConfig.TOPIC_CURVEDATA, KafkaConfig.EVENT_SAVE_CURVE, curveDTO);
    }

    public void initiateCurveFitting(CurveFittingRequestDTO curveFitRequest) {
        kafkaTemplate.send(KafkaConfig.TOPIC_CALCULATIONS, KafkaConfig.EVENT_REQUEST_CURVE_FIT, curveFitRequest);
    }

    public void sendResultData(ResultDataDTO resultData) {
        kafkaTemplate.send(KafkaConfig.TOPIC_RESULTDATA, KafkaConfig.EVENT_SAVE_RESULT_DATA, resultData);
    }

    public void sendResultFeatureStats(Long resultSetId, List<ResultFeatureStatDTO> resultFeatureStats) {
        for (ResultFeatureStatDTO resultFeatureStatDTO: resultFeatureStats) {
            kafkaTemplate.send(KafkaConfig.TOPIC_RESULTDATA, KafkaConfig.EVENT_SAVE_RESULT_STATS, resultFeatureStatDTO.withResultSetId(resultSetId));
        }
    }
    
    public void sendScriptExecutionRequest(ScriptExecutionInputDTO scriptRequest) {
    	kafkaTemplate.send(KafkaConfig.TOPIC_SCRIPTENGINE, KafkaConfig.EVENT_REQUEST_SCRIPT_EXECUTION, scriptRequest);
    }
}
