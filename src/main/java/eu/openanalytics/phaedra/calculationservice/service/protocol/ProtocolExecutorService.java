package eu.openanalytics.phaedra.calculationservice.service.protocol;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import eu.openanalytics.phaedra.calculationservice.model.CalculationContext;
import eu.openanalytics.phaedra.calculationservice.model.Sequence;
import eu.openanalytics.phaedra.platservice.client.PlateServiceClient;
import eu.openanalytics.phaedra.platservice.client.exception.PlateUnresolvableException;
import eu.openanalytics.phaedra.platservice.dto.WellDTO;
import eu.openanalytics.phaedra.protocolservice.client.exception.ProtocolUnresolvableException;
import eu.openanalytics.phaedra.resultdataservice.client.ResultDataServiceClient;
import eu.openanalytics.phaedra.resultdataservice.client.exception.ResultSetUnresolvableException;
import eu.openanalytics.phaedra.resultdataservice.dto.ResultSetDTO;
import eu.openanalytics.phaedra.resultdataservice.enumeration.StatusCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static eu.openanalytics.phaedra.calculationservice.service.protocol.ProtocolLogger.log;

@Service
public class ProtocolExecutorService {

    private final ThreadPoolExecutor executorService;
    private final ResultDataServiceClient resultDataServiceClient;
    private final SequenceExecutorService sequenceExecutorService;
    private final ProtocolInfoCollector protocolInfoCollector;
    private final PlateServiceClient plateServiceClient;

    private final Logger logger = LoggerFactory.getLogger(getClass());

    public ThreadPoolExecutor getExecutorService() {
        return executorService;
    }

    public ProtocolExecutorService(ResultDataServiceClient resultDataServiceClient, SequenceExecutorService sequenceExecutorService, ProtocolInfoCollector protocolInfoCollector, PlateServiceClient plateServiceClient) {
        this.resultDataServiceClient = resultDataServiceClient;
        this.sequenceExecutorService = sequenceExecutorService;
        this.protocolInfoCollector = protocolInfoCollector;
        this.plateServiceClient = plateServiceClient;

        var threadFactory = new ThreadFactoryBuilder().setNameFormat("protocol-exec-%s").build();
        executorService = new ThreadPoolExecutor(8, 1024, 60L, TimeUnit.SECONDS, new SynchronousQueue<>(), threadFactory);
    }

    public Future<ResultSetDTO> execute(long protocolId, long plateId, long measId) {
        // submit execution to the ThreadPool/ExecutorService and return a future
        return executorService.submit(() -> {
            return executeProtocol(protocolId, plateId, measId);
        });
    }

    public ResultSetDTO executeProtocol(long protocolId, long plateId, long measId) throws ProtocolUnresolvableException, ResultSetUnresolvableException, PlateUnresolvableException {
        // 1. get protocol
        logger.info("Preparing new calculation");
        final var protocol = protocolInfoCollector.getProtocol(protocolId);
        final var plate = plateServiceClient.getPlate(plateId);
        final var wellsSorted = plateServiceClient.getWellsOfPlateSorted(plateId);
        final var welltypesSorted = wellsSorted.stream().map(WellDTO::getWelltype).toList();
        final var uniqueWelltypes = new LinkedHashSet<>(welltypesSorted);

        // 2. create CalculationContext
        final var resultSet = resultDataServiceClient.createResultDataSet(protocolId, plateId, measId);
        final var cctx = CalculationContext.newInstance(plate, protocol, resultSet.getId(), measId, welltypesSorted, uniqueWelltypes);

        log(logger, cctx, "Starting calculation");

        // 3. sequentially execute every sequence
        for (var seq = 0; seq < protocol.getSequences().size(); seq++) {
            Sequence currentSequence = protocol.getSequences().get(seq);
            if (currentSequence == null) {
                cctx.getErrorCollector().handleError("executing protocol => missing sequence", seq);
                return saveError(resultSet, cctx.getErrorCollector());
            }
            var success = sequenceExecutorService.executeSequence(cctx, executorService, currentSequence);

            // 4. check for errors
            if (!success) {
                // if error -> stop executing sequences
                break;
            }

            // 5. no errors -> continue processing sequences
        }

        log(logger, cctx, "Waiting for FeatureStats to finish");

        // 6. wait for FeatureStats to be calculated
        // we can wait for all featureStats (of all sequences) here since nothing in the protocol depends on them
        for (var featureStat : cctx.getComputedStatsForFeature().entrySet()) {
            try {
                featureStat.getValue().get();
            } catch (InterruptedException e) {
                // ideally these exceptions should be caught and handled in the FeatureStatExecutor service, however
                // we still need to catch them here, because of the API design of Future.
                cctx.getErrorCollector().handleError("executing protocol => waiting for calculations of featureStats of a feature to complete => interrupted", e, featureStat.getKey());
            } catch (ExecutionException e) {
                cctx.getErrorCollector().handleError("executing protocol => waiting for calculations of featureStats of a feature to complete => exception during execution", e.getCause(), featureStat.getKey());
            } catch (Throwable e) {
                cctx.getErrorCollector().handleError("executing protocol => waiting for calculations of featureStats of a feature to complete => exception during execution", e, featureStat.getKey());
            }
        }

        // 7. check for errors
        if (cctx.getErrorCollector().hasError()) {
            return saveError(resultSet, cctx.getErrorCollector());
        }

        // 8. set ResultData status
        log(logger, cctx, "Calculation finished: SUCCESS");
        return resultDataServiceClient.completeResultDataSet(resultSet.getId(), StatusCode.SUCCESS, new ArrayList<>(), "");
    }

    private ResultSetDTO saveError(ResultSetDTO resultSet, ErrorCollector errorCollector) throws ResultSetUnresolvableException {
        logger.warn("Protocol failed with errorDescription\n" + errorCollector.getErrorDescription());
        return resultDataServiceClient.completeResultDataSet(resultSet.getId(), StatusCode.FAILURE, errorCollector.getErrors(), errorCollector.getErrorDescription());
    }

}
