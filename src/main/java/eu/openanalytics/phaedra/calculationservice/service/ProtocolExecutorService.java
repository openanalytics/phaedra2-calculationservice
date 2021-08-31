package eu.openanalytics.phaedra.calculationservice.service;

import eu.openanalytics.phaedra.calculationservice.controller.clients.ProtocolServiceClient;
import eu.openanalytics.phaedra.calculationservice.controller.clients.ProtocolUnresolvableException;
import eu.openanalytics.phaedra.calculationservice.controller.clients.ResultDataServiceClient;
import eu.openanalytics.phaedra.calculationservice.controller.clients.ResultSetUnresolvableException;
import eu.openanalytics.phaedra.calculationservice.dto.external.ErrorDTO;
import eu.openanalytics.phaedra.calculationservice.model.Protocol;
import eu.openanalytics.phaedra.calculationservice.model.Error;
import eu.openanalytics.phaedra.calculationservice.model.Sequence;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Service
public class ProtocolExecutorService {

    private final ProtocolServiceClient protocolServiceClient;
    private final ThreadPoolExecutor executorService;
    private final ResultDataServiceClient resultDataServiceClient;
    private final SequenceExecutorService sequenceExecutorService;

    public ProtocolExecutorService(ProtocolServiceClient protocolServiceClient, ResultDataServiceClient resultDataServiceClient, SequenceExecutorService sequenceExecutorService) {
        this.protocolServiceClient = protocolServiceClient;
        this.resultDataServiceClient = resultDataServiceClient;
        this.sequenceExecutorService = sequenceExecutorService;

        executorService = new ThreadPoolExecutor(8, 1024, 60L, TimeUnit.SECONDS, new SynchronousQueue<>());
    }

    public Future<?> execute(long protocolId, long plateId, long measId) {
        // submit execution to the ThreadPool/ExecutorService and return a future
        return executorService.submit(() -> {
            executeProtocol(protocolId, plateId, measId);
            return true;
        });
    }

    private void executeProtocol(long protocolId, long plateId, long measId) {
        try {
            // 1. get protocol
            Protocol protocol = protocolServiceClient.getProtocol(protocolId);

            // 2. create ResultSet
            var resultSet = resultDataServiceClient.createResultDataSet(protocolId, plateId, measId);
            var errorCollector = new ErrorCollector();

            // 3. sequentially execute every sequence
            for (var seq = 0; seq < protocol.getSequences().size(); seq++) {
                Sequence currentSequence = protocol.getSequences().get(seq);
                var success = sequenceExecutorService.executeSequence(executorService, errorCollector, currentSequence, measId, resultSet);

                // 4. check for errors
                if (!success) {
                    System.out.println(errorCollector.getErrorDescription());
                    resultDataServiceClient.completeResultDataSet(resultSet.getId(), "Error", map(errorCollector.getErrors()), errorCollector.getErrorDescription());
                    return;
                }

                // 5. no errors -> continue processing sequences
            }

            // 6. set ResultData status
            resultDataServiceClient.completeResultDataSet(resultSet.getId(), "Completed", new ArrayList<>(), "");
        } catch (ProtocolUnresolvableException | ResultSetUnresolvableException e) {
            e.printStackTrace();
        }
    }
    
    private List<ErrorDTO> map(List<Error> errors) {
        return errors.stream().map((e) -> new ErrorDTO(
                e.getTimestamp(),
                e.getExceptionClassName(),
                e.getExceptionMessage(),
                e.getDescription(),
                e.getFeatureId(),
                e.getFeatureName(),
                e.getSequenceNumber(),
                e.getFormulaId(),
                e.getFormulaName(),
                e.getCivType(),
                e.getCivVariableName(),
                e.getCivSource(),
                e.getExitCode(),
                e.getStatusMessage()
        )).collect(Collectors.toList());
    }

}
