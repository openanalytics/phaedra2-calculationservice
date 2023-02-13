package eu.openanalytics.phaedra.calculationservice.service;

import eu.openanalytics.phaedra.calculationservice.service.protocol.ProtocolExecutorService;
import eu.openanalytics.phaedra.commons.dto.CalculationRequestDTO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static eu.openanalytics.phaedra.calculationservice.config.KafkaConsumerConfig.PLATE_TOPIC;

@Service
public class KafkaConsumerService {

    @Autowired
    private ProtocolExecutorService protocolExecutorService;
    private final Logger logger = LoggerFactory.getLogger(getClass());

    @KafkaListener(topics = PLATE_TOPIC, groupId = "calculation-service", filter = "plateCalculationEventFilter")
    public void onPlateCalculationEvent(CalculationRequestDTO calculationRequestDTO, @Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) String msgKey) throws ExecutionException, InterruptedException {
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
}
