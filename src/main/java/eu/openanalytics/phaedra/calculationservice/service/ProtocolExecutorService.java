package eu.openanalytics.phaedra.calculationservice.service;

import eu.openanalytics.phaedra.calculationservice.model.Protocol;
import eu.openanalytics.phaedra.calculationservice.model.Sequence;
import eu.openanalytics.phaedra.protocolservice.client.exception.ProtocolUnresolvableException;
import eu.openanalytics.phaedra.resultdataservice.client.ResultDataServiceClient;
import eu.openanalytics.phaedra.resultdataservice.client.exception.ResultSetUnresolvableException;
import eu.openanalytics.phaedra.resultdataservice.dto.ResultSetDTO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

@Service
public class ProtocolExecutorService {

    private final ThreadPoolExecutor executorService;
    private final ResultDataServiceClient resultDataServiceClient;
    private final SequenceExecutorService sequenceExecutorService;
    private final ProtocolInfoCollector protocolInfoCollector;

    private final Logger logger = LoggerFactory.getLogger(getClass());

    public ProtocolExecutorService(ResultDataServiceClient resultDataServiceClient, SequenceExecutorService sequenceExecutorService, ProtocolInfoCollector protocolInfoCollector) {
        this.resultDataServiceClient = resultDataServiceClient;
        this.sequenceExecutorService = sequenceExecutorService;
        this.protocolInfoCollector = protocolInfoCollector;

        executorService = new ThreadPoolExecutor(8, 1024, 60L, TimeUnit.SECONDS, new SynchronousQueue<>());
    }

    public Future<ResultSetDTO> execute(long protocolId, long plateId, long measId) {
        // submit execution to the ThreadPool/ExecutorService and return a future
        return executorService.submit(() -> {
            return executeProtocol(protocolId, plateId, measId);
        });
    }

    private ResultSetDTO executeProtocol(long protocolId, long plateId, long measId) throws ProtocolUnresolvableException, ResultSetUnresolvableException {
        // 1. get protocol
        // TODO handle these errors
        Protocol protocol = protocolInfoCollector.getProtocol(protocolId);

        // 2. create ResultSet
        var resultSet = resultDataServiceClient.createResultDataSet(protocolId, plateId, measId);
        var errorCollector = new ErrorCollector();

        // 3. sequentially execute every sequence
        for (var seq = 0; seq < protocol.getSequences().size(); seq++) {
            Sequence currentSequence = protocol.getSequences().get(seq);
            if (currentSequence == null) {
                errorCollector.handleError("executing protocol => missing sequence", seq);
                return saveError(resultSet, errorCollector);
            }
            var success = sequenceExecutorService.executeSequence(executorService, errorCollector, currentSequence, measId, resultSet);

            // 4. check for errors
            if (!success) {
                return saveError(resultSet, errorCollector);
            }

            // 5. no errors -> continue processing sequences
        }

        // 6. set ResultData status
        return resultDataServiceClient.completeResultDataSet(resultSet.getId(), "Completed", new ArrayList<>(), "");
    }

    private ResultSetDTO saveError(ResultSetDTO resultSet, ErrorCollector errorCollector) throws ResultSetUnresolvableException {
        logger.warn("Protocol failed with errorDescription\n" + errorCollector.getErrorDescription());
        return resultDataServiceClient.completeResultDataSet(resultSet.getId(), "Error", errorCollector.getErrors(), errorCollector.getErrorDescription());
    }

}
