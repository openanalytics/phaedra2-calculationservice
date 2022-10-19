/**
 * Phaedra II
 *
 * Copyright (C) 2016-2022 Open Analytics
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
package eu.openanalytics.phaedra.calculationservice.service.protocol;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import eu.openanalytics.curvedataservice.dto.CurveDTO;
import eu.openanalytics.phaedra.calculationservice.dto.DRCInputDTO;
import eu.openanalytics.phaedra.calculationservice.model.CurveFittingContext;
import eu.openanalytics.phaedra.calculationservice.model.SuccessTracker;
import eu.openanalytics.phaedra.curvedataservice.client.CurveDataServiceClient;
import eu.openanalytics.phaedra.curvedataservice.client.exception.CurveUnresolvedException;
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
import eu.openanalytics.phaedra.scriptengine.dto.ScriptExecutionOutputDTO;
import eu.openanalytics.phaedra.util.WellNumberUtils;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.math3.util.Precision;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.*;
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
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final ObjectMapper objectMapper = new ObjectMapper(); // TODO thread-safe?
    private final Logger logger = LoggerFactory.getLogger(getClass());

    public CurveFittingExecutorService(ScriptEngineClient scriptEngineClient, ResultDataServiceClient resultDataServiceClient,
                                       PlateServiceClient plateServiceClient, ProtocolServiceClient protocolServiceClient,
                                       KafkaTemplate<String, Object> kafkaTemplate) {
        this.scriptEngineClient = scriptEngineClient;
        this.resultDataServiceClient = resultDataServiceClient;
        this.plateServiceClient = plateServiceClient;
        this.protocolServiceClient = protocolServiceClient;
        this.kafkaTemplate = kafkaTemplate;

        var threadFactory = new ThreadFactoryBuilder().setNameFormat("protocol-exec-%s").build();
        this.executorService = new ThreadPoolExecutor(8, 1024, 60L, TimeUnit.SECONDS, new SynchronousQueue<>(), threadFactory);
    }

    public record CurveFittingExecution(CompletableFuture<Long> curveId, Future<List<CurveDTO>> plateCurves) {};

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

    public void onCurveFitFeature(CurveDTO curveDTO) {

    }

    private List<CurveDTO> executeCurveFitting(CompletableFuture<Long> curveIdFuture, long protocolId, long plateId, long resultSetId, long measId) throws ProtocolUnresolvableException, ResultSetUnresolvableException, PlateUnresolvableException, CurveUnresolvedException {
        var plate  = plateServiceClient.getPlate(plateId);
        var wells = plateServiceClient.getWells(plateId);

        var protocolFeatures = protocolServiceClient.getFeaturesOfProtocol(protocolId);
        logger.info("Number of feature within protocol " + protocolId + " : " + protocolFeatures.size());

        var curveFeatures = protocolFeatures.stream().filter(pf -> pf.getDrcModel() != null).collect(Collectors.toList());
        logger.info("Number of feature with curve fitting models: " + curveFeatures.size());

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
        logger.info("Number of unique substances for plate " + plate + " is " + wellSubstancesUnique.size());

        if (CollectionUtils.isEmpty(wellSubstancesUnique))
            return null; //TODO: Return a proper error

        List<Object[]> curvesToFit = new ArrayList<>();
        for (FeatureDTO feature: curveFeatures) {
            for (String wellSubstance: wellSubstancesUnique) {
                curvesToFit.add(new Object[] { wellSubstance, feature.getId() });
            }
        }
        logger.info("Number of curve to be fitted: " + curvesToFit.size());
        var cfCtx = CurveFittingContext.newInstance(plate, wells, wellSubstances, wellSubstancesUnique, curveFeatures, resultSetId, protocolId);

        List<CurveDTO> results = new ArrayList<>();
        for (Object[] o : curvesToFit) {
            String substance = (String) o[0];
            long featureId = (long) o[1];
            logger.info("Fit curve for substance " + substance + " and featureId " + featureId);
            DRCInputDTO drcInput = collectDRCIntpuData(cfCtx, substance, featureId);
            Optional<ScriptExecution> execution = fitCurve(drcInput);
            if (execution.isPresent()) {
                try {
                    ScriptExecutionOutputDTO outputDTO = execution.get().getOutput().get();
                    if (StringUtils.isNotBlank(outputDTO.getOutput())) {
                        logger.info("Output is " + outputDTO.getOutput());

                        DRCOutput drcOutput = objectMapper.readValue(outputDTO.getOutput(), DRCOutput.class);
                        if (drcOutput.output != null) {
                            createNewCurve(drcInput, drcOutput);
//                            createNewCurve(substance, plateId, protocolId, featureId, resultSetId, drcOutput.output.dose, drcOutput.output.prediction);
                        }
                    } else {
                        logger.info("Not output is created!!");
                    }
                } catch (InterruptedException e) {
                    //TODO: Process error correctly
                    logger.error("No curve is created due to " + e.getMessage());
                } catch (ExecutionException e) {
                    //TODO: Process error correctly
                    logger.error("No curve is created due to " + e.getMessage());
                } catch (JsonMappingException e) {
                    //TODO: Process error correctly
                    logger.error("No curve is created due to " + e.getMessage());
                } catch (JsonProcessingException e) {
                    //TODO: Process error correctly
                    logger.error("No curve is created due to " + e.getMessage());
                }
            }
        }

        return results;
    }

    public void createNewCurve(DRCInputDTO drcInput, DRCOutput drcOutput) {
        CurveDTO curveDTO = CurveDTO.builder()
                .substanceName(drcInput.getSubstance())
                .plateId(drcInput.getPlateId())
                .protocolId(drcInput.getProtocolId())
                .featureId(drcInput.getFeatureId())
                .resultSetId(drcInput.getResultSetId())
                .wells(drcInput.getWells())
                .featureValues(drcInput.getValues())
                .fitDate(new Date())
                .version("0.0.1")
                .plotDoseData(drcOutput.output.dose)
                .plotPredictionData(drcOutput.output.prediction)
                .build();
        kafkaTemplate.send("curvedata-topic", "createCurve", curveDTO);
    }

//    public void createNewCurve(String substance, Long plateId, Long protocolId, Long featureId, Long resultSetId, float[] dose, float[] prediction) {
//        CurveDTO curveDTO = CurveDTO.builder()
//                .substanceName(substance)
//                .plateId(plateId)
//                .protocolId(protocolId)
//                .featureId(featureId)
//                .resultSetId(resultSetId)
//                .fitDate(new Date())
//                .version("0.0.1")
//                .plotDoseData(dose)
//                .plotPredictionData(prediction)
//                .build();
//        kafkaTemplate.send("curvedata-topic", "createCurve", curveDTO);
//    }

    private DRCInputDTO collectDRCIntpuData(CurveFittingContext cfCtx, String substanceName, long featureId) {
        try {
            var featureResult = resultDataServiceClient.getResultData(cfCtx.getResultSetId(), featureId);

            var wells = cfCtx.getWells().stream()
                    .filter(w -> w.getWellSubstance() != null && w.getWellSubstance().getName().equals(substanceName))
                    .collect(Collectors.toList());
            var drcModelDTO = cfCtx.getCurveFeatures().stream()
                    .filter(f -> f.getId() == featureId)
                    .findFirst()
                    .map(f -> f.getDrcModel());


            long[] wellIds = new long[wells.size()];
            float[] values = new float[wells.size()];
            float[] concs = new float[wells.size()];
            float[] accepts = new float[wells.size()];

            for (int i = 0; i < wells.size(); i++) {
                // Set the well id
                wellIds[i] = wells.get(i).getId();

                // Set the well substance concentration value
                float conc = wells.get(i).getWellSubstance().getConcentration().floatValue();
                concs[i] = (float) Precision.round(-Math.log10(conc), 3);

                // Set the well accept value (true or false)
                accepts[i] = (wells.get(i).getStatus().getCode() >= 0 && cfCtx.getPlate().getValidationStatus().getCode() >= 0 && cfCtx.getPlate().getApprovalStatus().getCode() >= 0) ? 1 : 0;

                // Set the well feature value
                var valueIndex = WellNumberUtils.getWellNr(wells.get(i).getRow(), wells.get(i).getColumn(), cfCtx.getPlate().getColumns()) - 1;
                values[i] = featureResult.getValues()[valueIndex];
            }

            return DRCInputDTO.builder()
                    .substance(substanceName)
                    .plateId(cfCtx.getPlate().getId())
                    .featureId(featureId)
                    .protocolId(cfCtx.getProtocolId())
                    .resultSetId(cfCtx.getResultSetId())
                    .wells(wellIds)
                    .values(values)
                    .concs(concs)
                    .accepts(accepts)
                    .drcModel(drcModelDTO)
                    .build();
        } catch (ResultDataUnresolvableException e) {
            throw new RuntimeException(e);
        }
    }

    public Optional<ScriptExecution> fitCurve(DRCInputDTO inputDTO) {
        try {
            logger.info("Fitting curve for substance " + inputDTO.getSubstance() + " and featureId " + inputDTO.getFeatureId());
//            var wells = cfCtx.getWells().stream()
//                    .filter(w -> w.getWellSubstance() != null && w.getWellSubstance().getName().equals(substanceName))
//                    .collect(Collectors.toList());
//            var drcModelDTO = cfCtx.getCurveFeatures().stream()
//                    .filter(f -> f.getId() == featureId)
//                    .findFirst()
//                    .map(f -> f.getDrcModel());
//
//            var featureResult = resultDataServiceClient.getResultData(cfCtx.getResultSetId(), featureId);
//
//            double[] values = new double[wells.size()];
//            double[] concs = new double[wells.size()];
//            double[] accepts = new double[wells.size()];
//
//            for (int i = 0; i < wells.size(); i++) {
//                // Set the well substance concentration value
//                double conc = wells.get(i).getWellSubstance().getConcentration();
//                concs[i] = Precision.round(-Math.log10(conc), 3);
//
//                // Set the well accept value (true or false)
//                accepts[i] = (wells.get(i).getStatus().getCode() >= 0 && cfCtx.getPlate().getValidationStatus().getCode() >= 0 && cfCtx.getPlate().getApprovalStatus().getCode() >= 0) ? 1 : 0;
//
//                // Set the well feature value
//                var valueIndex = WellNumberUtils.getWellNr(wells.get(i).getRow(), wells.get(i).getColumn(), cfCtx.getPlate().getColumns()) - 1;
//                values[i] = featureResult.getValues()[valueIndex];
//            }

            var inputVariables = new HashMap<String, Object>();

            inputVariables.put("doses", inputDTO.getConcs());
            inputVariables.put("responses", inputDTO.getValues());
            inputVariables.put("accepts", inputDTO.getAccepts());

            var slope  = inputDTO.getDrcModel().isPresent() ? inputDTO.getDrcModel().get().getSlope() : "ascending";
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
                    "\tslope = \""+ slope +"\"\n" +
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
        }
        return Optional.empty();
    }

    public Optional<DataPredict2Plot> getOutputData(CurveFittingContext cfCtx, FeatureCurvFitting curvFitting) {
        if (curvFitting.getOutput().isEmpty()) {
            return Optional.empty();
        }

        var output = curvFitting.getOutput().get();
        try {
            OutputWrapper outputValue = objectMapper.readValue(output.getOutput(), OutputWrapper.class);
            if (StringUtils.isNotBlank(outputValue.output)) {
                var dataPredict2Plot = objectMapper.readValue(outputValue.output, DataPredict2Plot.class);
                if (dataPredict2Plot != null) {
                    return Optional.of(dataPredict2Plot);
                }
            }

        } catch (JsonProcessingException e) {
            //TODO: set error message
        }
        return Optional.empty();
    }

    private static class OutputWrapper {

        public final String output;

        @JsonCreator
        private OutputWrapper(@JsonProperty(value = "output", required = true) String output) {
            this.output = output;
        }
    }

    private static class DRCOutput {
        public DataPredict2Plot output;

        @JsonCreator
        private DRCOutput(@JsonProperty(value = "output", required = true) DataPredict2Plot output) {
            this.output = output;
        }
    }
    private static class DataPredict2Plot {

        public float[] dose;
        public float[] prediction;
        public float[] lower;
        public float[] upper;

        @JsonCreator
        private DataPredict2Plot(@JsonProperty(value = "dose", required = true) float[] dose,
                                 @JsonProperty(value = "Prediction", required = true) float[] prediction,
                                 @JsonProperty(value = "Lower", required = true) float[] lower,
                                 @JsonProperty(value = "Upper", required = true) float[] upper) {
            this.dose = dose;
            this.prediction = prediction;
            this.lower = lower;
            this.upper = upper;
        }
    }

}
