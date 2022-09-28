package eu.openanalytics.phaedra.calculationservice.service.protocol;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import eu.openanalytics.curvedataservice.dto.CurveDTO;
import eu.openanalytics.phaedra.calculationservice.model.CurveFittingContext;
import eu.openanalytics.phaedra.plateservice.client.PlateServiceClient;
import eu.openanalytics.phaedra.plateservice.client.exception.PlateUnresolvableException;
import eu.openanalytics.phaedra.plateservice.dto.WellSubstanceDTO;
import eu.openanalytics.phaedra.protocolservice.client.ProtocolServiceClient;
import eu.openanalytics.phaedra.protocolservice.client.exception.ProtocolUnresolvableException;
import eu.openanalytics.phaedra.protocolservice.dto.FeatureDTO;
import eu.openanalytics.phaedra.resultdataservice.client.ResultDataServiceClient;
import eu.openanalytics.phaedra.resultdataservice.client.exception.ResultDataUnresolvableException;
import eu.openanalytics.phaedra.resultdataservice.client.exception.ResultSetUnresolvableException;
import eu.openanalytics.phaedra.scriptengine.client.ScriptEngineClient;
import eu.openanalytics.phaedra.scriptengine.client.model.ScriptExecution;
import eu.openanalytics.phaedra.util.WellNumberUtils;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.math3.util.Precision;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.*;
import java.util.stream.Collectors;

import static eu.openanalytics.phaedra.calculationservice.CalculationService.R_FAST_LANE;

@Service
public class CurveFittingExecutorService {
    private final ScriptEngineClient scriptEngineClient;
    private final ResultDataServiceClient resultDataServiceClient;
    private final ThreadPoolExecutor executorService;
    private final PlateServiceClient plateServiceClient;
    private final ProtocolServiceClient protocolServiceClient;
    private final ObjectMapper objectMapper = new ObjectMapper(); // TODO thread-safe?
    private final Logger logger = LoggerFactory.getLogger(getClass());

    public CurveFittingExecutorService(ScriptEngineClient scriptEngineClient, ResultDataServiceClient resultDataServiceClient,
                                       PlateServiceClient plateServiceClient, ProtocolServiceClient protocolServiceClient) {
        this.scriptEngineClient = scriptEngineClient;
        this.resultDataServiceClient = resultDataServiceClient;
        this.plateServiceClient = plateServiceClient;
        this.protocolServiceClient = protocolServiceClient;

        var threadFactory = new ThreadFactoryBuilder().setNameFormat("protocol-exec-%s").build();
        this.executorService = new ThreadPoolExecutor(8, 1024, 60L, TimeUnit.SECONDS, new SynchronousQueue<>(), threadFactory);
    }

    public record CurveFittingExecution(CompletableFuture<Long> curveId, Future<CurveDTO> curve) {};

    public CurveFittingExecution execute(long protocolId, long plateId, long resultSetId, long measId) {
        // submit execution to the ThreadPool/ExecutorService and return a future
        var curveIdFuture = new CompletableFuture<Long>();
        return new CurveFittingExecution(curveIdFuture, executorService.submit(() -> {
            try {
                logger.info("Start curve fitting with protocol " + protocolId + ", plate " + plateId + ", and resultSet " + resultSetId);
                return executeCurveFitting(curveIdFuture, protocolId, plateId, resultSetId, measId);
            } catch (Throwable ex) {
                // print the stack strace. Since the future may never be awaited, we may not see the error otherwise
                ex.printStackTrace();
                throw ex;
            }
        }));
    }

    private CurveDTO executeCurveFitting(CompletableFuture<Long> curveIdFuture, long protocolId, long plateId, long resultSetId, long measId) throws ProtocolUnresolvableException, ResultSetUnresolvableException, PlateUnresolvableException {
        var plate  = plateServiceClient.getPlate(plateId);
        var wells = plateServiceClient.getWells(plateId);

        var protocolFeatures = protocolServiceClient.getFeaturesOfProtocol(protocolId);
        var curveFeatures = protocolFeatures.stream().filter(pf -> pf.getDrcModel() != null).collect(Collectors.toList());

        if (CollectionUtils.isEmpty(curveFeatures))
            return null; //TODO: Return a proper error

        var wellSubstances = plateServiceClient.getWellSubstances(plateId);
        var wellSubstancesUnique = wellSubstances
                .stream()
                .map(WellSubstanceDTO::getName)
                .collect(Collectors.toList())
                .stream()
                .distinct()
                .collect(Collectors.toList());

        if (CollectionUtils.isEmpty(wellSubstancesUnique))
            return null; //TODO: Return a proper error

        List<Object[]> curvesToFit = new ArrayList<>();
        for (FeatureDTO feature: curveFeatures) {
            for (String wellSubstance: wellSubstancesUnique) {
                curvesToFit.add(new Object[] { wellSubstance, feature.getId() });
            }
        }

        var cfCtx = CurveFittingContext.newInstance(plate, wells, wellSubstances, wellSubstancesUnique, curveFeatures, resultSetId);

        for (Object[] o : curvesToFit) {
            String substance = (String) o[0];
            long featureId = (long) o[1];
            fitCurve(cfCtx, substance, featureId);
        }

        return new CurveDTO(null, protocolId, plateId, resultSetId);
    }

    public Optional<ScriptExecution> fitCurve(CurveFittingContext cfCtx, String substanceName, long featureId) {
        try {
            var wells = cfCtx.getWells().stream().filter(w -> w.getWellSubstance().getName().equals(substanceName)).collect(Collectors.toList());
            var curveSettings = cfCtx.getCurveFeatures().stream().filter(f -> f.getId() == featureId).findFirst();
            var featureResult = resultDataServiceClient.getResultData(cfCtx.getResultSetId(), featureId);

            double[] values = new double[wells.size()];
            double[] concs = new double[wells.size()];
            boolean[] accepts = new boolean[wells.size()];

            for (int i = 0; i < wells.size(); i++) {
                // Set the well substance concentration value
                double conc = wells.get(i).getWellSubstance().getConcentration();
                concs[i] = Precision.round(-Math.log10(conc), 3);

                // Set the well accept value (true or false)
                accepts[i] = wells.get(i).getStatus().getCode() >= 0 && cfCtx.getPlate().getValidationStatus().getCode() >= 0 && cfCtx.getPlate().getApprovalStatus().getCode() >= 0;

                // Set the well feature value
                var valueIndex = WellNumberUtils.getWellNr(wells.get(i).getRow(), wells.get(i).getColumn(), cfCtx.getPlate().getColumns()) - 1;
                values[i] = featureResult.getValues()[valueIndex];
            }

            var inputVariables = new HashMap<String, Object>();

            inputVariables.put("doses", List.of(concs));
            inputVariables.put("responses", List.of(values));
            inputVariables.put("accepts", List.of(accepts));

            var script = "library(receptor2)\n" +
                    "\n" +
                    "dose <- input$doses\n" +
                    "response <- input$responses\n" +
                    "accept <- input$accepts\n" +
                    "\n" +
                    "value <- fittingLogisticModel(\n" +
                    "\tinputData = data.frame(dose, response),\n" +
                    "\taccept = accept,\n" +
                    "\tfixedBottom = 0.0,\n" +
                    "\tfixedTop = NA,\n" +
                    "\tfixedSlope = NA,\n" +
                    "\tconfLevel = 0.95,\n" +
                    "\trobustMethod = 'tukey',\n" +
                    "\tresponseName = 'Effect',\n" +
                    "\tslope = \"descending\"\n" +
                    ")\n" +
                    "\n" +
                    "output <- value$dataPredict2Plot";



            var execution = scriptEngineClient.newScriptExecution(
                    R_FAST_LANE,
                    script,
                    objectMapper.writeValueAsString(inputVariables)
            );

            scriptEngineClient.execute(execution);

            return Optional.of(execution);
        } catch (JsonProcessingException e) {
            // this error will probably never occur, see: https://stackoverflow.com/q/26716020/1393103 for examples where it does
//            cctx.getErrorCollector().handleError("executing feature => writing input variables and request", e, feature, feature.getFormula());
        } catch (ResultDataUnresolvableException e) {
            throw new RuntimeException(e);
        }
        return Optional.empty();
    }
}
