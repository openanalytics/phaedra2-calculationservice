package eu.openanalytics.phaedra.calculationservice.service;

import eu.openanalytics.phaedra.calculationservice.config.KafkaConfig;
import eu.openanalytics.phaedra.calculationservice.dto.CurveFittingRequestDTO;
import eu.openanalytics.phaedra.calculationservice.service.protocol.CurveFittingExecutorService;
import eu.openanalytics.phaedra.calculationservice.service.protocol.ProtocolExecutorService;
import eu.openanalytics.phaedra.commons.dto.CalculationRequestDTO;
import eu.openanalytics.phaedra.resultdataservice.dto.ResultDataDTO;
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

import static eu.openanalytics.phaedra.calculationservice.config.KafkaConfig.CALCULATIONS_TOPIC;
import static eu.openanalytics.phaedra.calculationservice.config.KafkaConfig.PLATE_TOPIC;

@Service
public class KafkaConsumerService {

    @Autowired
    private ProtocolExecutorService protocolExecutorService;
    @Autowired
    private CurveFittingExecutorService curveFittingExecutorService;
    private final Logger logger = LoggerFactory.getLogger(getClass());

    @KafkaListener(topics = PLATE_TOPIC, groupId = "calculation-service", filter = "plateCalculationEventFilter")
    public void onPlateCalculationEvent(CalculationRequestDTO calculationRequestDTO, @Header(KafkaHeaders.RECEIVED_KEY) String msgKey) throws ExecutionException, InterruptedException {
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

    @KafkaListener(topics = CALCULATIONS_TOPIC, groupId = "calculation-service", filter = "curveFitEventFilter")
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
