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

import java.util.concurrent.ExecutionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;

import eu.openanalytics.phaedra.calculationservice.config.KafkaConfig;
import eu.openanalytics.phaedra.calculationservice.dto.CalculationRequestDTO;
import eu.openanalytics.phaedra.calculationservice.dto.CurveFittingRequestDTO;
import eu.openanalytics.phaedra.calculationservice.service.protocol.CurveFittingExecutorService;
import eu.openanalytics.phaedra.calculationservice.service.protocol.ProtocolExecutorService;
import eu.openanalytics.phaedra.calculationservice.service.script.ScriptExecutionService;
import eu.openanalytics.phaedra.scriptengine.dto.ScriptExecutionOutputDTO;

@Service
public class KafkaConsumerService {

    @Autowired
    private ProtocolExecutorService protocolExecutorService;
    @Autowired
    private CurveFittingExecutorService curveFittingExecutorService;
    @Autowired
    private ScriptExecutionService scriptExecutionService;
    
    private final Logger logger = LoggerFactory.getLogger(getClass());

    @KafkaListener(topics = KafkaConfig.TOPIC_CALCULATIONS, groupId = KafkaConfig.GROUP_ID, filter = "requestPlateCalculationFilter")
    public void onRequestPlateCalculation(CalculationRequestDTO calculationRequestDTO, @Header(KafkaHeaders.RECEIVED_KEY) String msgKey) throws ExecutionException, InterruptedException {
        logger.info("calculation-service: received a plate calculation event!");
        protocolExecutorService.execute(
                calculationRequestDTO.getProtocolId(),
                calculationRequestDTO.getPlateId(),
                calculationRequestDTO.getMeasId());
    }

    @KafkaListener(topics = KafkaConfig.TOPIC_CALCULATIONS, groupId = KafkaConfig.GROUP_ID, filter = "requestCurveFitFilter")
    public void onCurveFitEvent(CurveFittingRequestDTO curveFittingRequestDTO) throws ExecutionException, InterruptedException {
        logger.info("calculation-service: received a curve fit event!");
        curveFittingExecutorService.execute(
                curveFittingRequestDTO.getPlateId(),
                curveFittingRequestDTO.getFeatureResultData());
    }
    
    @KafkaListener(topics = KafkaConfig.TOPIC_SCRIPTENGINE, groupId = KafkaConfig.GROUP_ID, filter = "scriptExecutionUpdateFilter")
    public void onScriptExecutionEvent(ScriptExecutionOutputDTO output, @Header(KafkaHeaders.RECEIVED_KEY) String key) {
		scriptExecutionService.handleScriptExecutionUpdate(output);
    }
}
