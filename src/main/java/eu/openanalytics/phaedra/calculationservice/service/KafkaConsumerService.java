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

import static eu.openanalytics.phaedra.calculationservice.config.KafkaConfig.GROUP_ID;
import static eu.openanalytics.phaedra.calculationservice.config.KafkaConfig.TOPIC_CALCULATIONS;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;

import eu.openanalytics.phaedra.calculationservice.dto.CalculationRequestDTO;
import eu.openanalytics.phaedra.calculationservice.dto.CurveFittingRequestDTO;
import eu.openanalytics.phaedra.calculationservice.service.protocol.CurveFittingExecutorService;
import eu.openanalytics.phaedra.calculationservice.service.protocol.ProtocolExecutorService;

@Service
public class KafkaConsumerService {

    @Autowired
    private ProtocolExecutorService protocolExecutorService;
    @Autowired
    private CurveFittingExecutorService curveFittingExecutorService;
    private final Logger logger = LoggerFactory.getLogger(getClass());

    @KafkaListener(topics = TOPIC_CALCULATIONS, groupId = GROUP_ID, filter = "requestPlateCalculation")
    public void onRequestPlateCalculation(CalculationRequestDTO calculationRequestDTO, @Header(KafkaHeaders.RECEIVED_KEY) String msgKey) throws ExecutionException, InterruptedException {
        logger.info("calculation-service: received a plate calculation event!");
        var future = protocolExecutorService.execute(
                calculationRequestDTO.getProtocolId(),
                calculationRequestDTO.getPlateId(),
                calculationRequestDTO.getMeasId());
        Long timeout = 60000L;
        try {
            future.resultSet().get(timeout, TimeUnit.MILLISECONDS);
        } catch (TimeoutException ex) {

        }
    }

    @KafkaListener(topics = TOPIC_CALCULATIONS, groupId = GROUP_ID, filter = "curveFitEventFilter")
    public void onCurveFitEvent(CurveFittingRequestDTO curveFittingRequestDTO) throws ExecutionException, InterruptedException {
        logger.info("calculation-service: received a curve fit event!");
        var future = curveFittingExecutorService.execute(
                curveFittingRequestDTO.getPlateId(),
                curveFittingRequestDTO.getFeatureResultData());
        Long timeout = 60000L;
        try {
            future.plateCurves().get(timeout, TimeUnit.MILLISECONDS);
        } catch (TimeoutException ex) {

        }
    }

//    @KafkaListener(topics = KafkaConfig.RESULTDATA_TOPIC, groupId = "calculation-service", filter = "saveResultDataEventFilter")
//    public void onSaveResultDataEvent(ResultDataDTO resultDataDTO) {
//        logger.info("calculation-service: received a save result data event!");
//    }
}
