package eu.openanalytics.phaedra.calculationservice.service;

import eu.openanalytics.phaedra.calculationservice.service.protocol.ProtocolExecutorService;
import eu.openanalytics.phaedra.resultdataservice.client.ResultDataServiceClient;
import eu.openanalytics.phaedra.resultdataservice.client.exception.ResultSetUnresolvableException;
import eu.openanalytics.phaedra.resultdataservice.dto.ErrorDTO;
import eu.openanalytics.phaedra.resultdataservice.enumeration.StatusCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.concurrent.ExecutionException;

@Service
public class CalculationRecoveryService {
    private final Logger logger = LoggerFactory.getLogger(getClass());

    private final ResultDataServiceClient resultDataServiceClient;
    private final ProtocolExecutorService protocolExecutorService;

    private static final Duration MAX_RECOVER_TIME = Duration.ofHours(2);

    public CalculationRecoveryService(ResultDataServiceClient resultDataServiceClient, ProtocolExecutorService protocolExecutorService, TokenService tokenService) {
        this.resultDataServiceClient = resultDataServiceClient;
        this.protocolExecutorService = protocolExecutorService;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void recoverCalculations() throws ResultSetUnresolvableException, ExecutionException, InterruptedException {
        var startTime = LocalDateTime.now().minus(MAX_RECOVER_TIME);
        var resultSets = resultDataServiceClient
                .getResultSet(StatusCode.SCHEDULED)
                .stream().filter(it -> it.getExecutionStartTimeStamp().isAfter(startTime))
                .toList();

        logger.info("Found {} calculation to retry", resultSets.size());

        for (var resultSet : resultSets) {
            var r = protocolExecutorService.execute(resultSet.getProtocolId(), resultSet.getPlateId(), resultSet.getMeasId());
            var newResultSetId = r.resultSetId().get();
            var error = ErrorDTO.builder()
                    .timestamp(LocalDateTime.now())
                    .description("Calculation re-scheduled because CalculationService was restarted")
                    .newResultSetId(newResultSetId)
                    .build();
            resultDataServiceClient.completeResultDataSet(resultSet.getId(), StatusCode.FAILURE,
                    List.of(error), error.toString());
        }

    }
}
