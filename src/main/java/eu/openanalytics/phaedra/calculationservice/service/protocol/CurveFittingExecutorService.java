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
package eu.openanalytics.phaedra.calculationservice.service.protocol;

import static java.lang.Float.NaN;
import static java.lang.Float.parseFloat;
import static org.apache.commons.lang3.StringUtils.isNotBlank;
import static org.apache.commons.lang3.math.NumberUtils.isCreatable;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.math3.util.Precision;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import eu.openanalytics.curvedataservice.dto.CurveDTO;
import eu.openanalytics.phaedra.calculationservice.dto.DRCInputDTO;
import eu.openanalytics.phaedra.calculationservice.enumeration.ScriptLanguage;
import eu.openanalytics.phaedra.calculationservice.model.CurveFittingContext;
import eu.openanalytics.phaedra.calculationservice.service.KafkaProducerService;
import eu.openanalytics.phaedra.calculationservice.service.script.ScriptExecutionRequest;
import eu.openanalytics.phaedra.calculationservice.service.script.ScriptExecutionService;
import eu.openanalytics.phaedra.plateservice.client.PlateServiceClient;
import eu.openanalytics.phaedra.plateservice.client.exception.PlateUnresolvableException;
import eu.openanalytics.phaedra.plateservice.dto.WellSubstanceDTO;
import eu.openanalytics.phaedra.protocolservice.client.ProtocolServiceClient;
import eu.openanalytics.phaedra.protocolservice.client.exception.FeatureUnresolvableException;
import eu.openanalytics.phaedra.resultdataservice.dto.ResultDataDTO;
import eu.openanalytics.phaedra.scriptengine.dto.ScriptExecutionOutputDTO;
import eu.openanalytics.phaedra.util.WellNumberUtils;

@Service
public class CurveFittingExecutorService {
	
    private final PlateServiceClient plateServiceClient;
    private final ProtocolServiceClient protocolServiceClient;
    
    private final KafkaProducerService kafkaProducerService;
    private final ScriptExecutionService scriptExecutionService;
    
    private final ObjectMapper objectMapper;
    private final ExecutorService executorService;
    private final Logger logger = LoggerFactory.getLogger(getClass());

    public CurveFittingExecutorService(
    		PlateServiceClient plateServiceClient,
    		ProtocolServiceClient protocolServiceClient,
    		KafkaProducerService kafkaProducerService,
    		ScriptExecutionService scriptExecutionService,
    		ObjectMapper objectMapper) {
    	
        this.plateServiceClient = plateServiceClient;
        this.protocolServiceClient = protocolServiceClient;
        
        this.kafkaProducerService = kafkaProducerService;
        this.scriptExecutionService = scriptExecutionService;
        
        this.objectMapper = objectMapper;
        
        executorService = Executors.newCachedThreadPool();
    }

    public record CurveFittingExecution(CompletableFuture<Long> curveId, Future<List<CurveDTO>> curves) {};

    public CurveFittingExecution execute(long plateId, ResultDataDTO featureResultData) {
        var curveIdFuture = new CompletableFuture<Long>();
        return new CurveFittingExecution(curveIdFuture, executorService.submit(() -> {
            try {
                return executeCurveFit(curveIdFuture, plateId, featureResultData);
            } catch (Throwable ex) {
                // print the stack strace. Since the future may never be awaited, we may not see the error otherwise
                ex.printStackTrace();
                throw ex;
            }
        }));
    }

    private List<CurveDTO> executeCurveFit(CompletableFuture<Long> curveIdFuture, long plateId, ResultDataDTO featureResultData) throws PlateUnresolvableException, FeatureUnresolvableException {
        var plate  = plateServiceClient.getPlate(plateId);
        var wells = plateServiceClient.getWells(plateId);

        if (featureResultData == null) {
            logger.info("Feature result data is null!!");
            return null;
        }

        logger.info("Get feature by featureId: " + featureResultData.getFeatureId());
        var feature = protocolServiceClient.getFeature(featureResultData.getFeatureId());
        if (feature.getDrcModel() == null) {
            logger.info("No drcModel found featureId: " + featureResultData.getFeatureId());
            return null;
        }

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
            for (String wellSubstance: wellSubstancesUnique) {
                curvesToFit.add(new Object[] { wellSubstance, feature.getId() });
            }
        var cfCtx = CurveFittingContext.newInstance(plate, wells, wellSubstances, wellSubstancesUnique, feature, feature.getDrcModel());

        List<CurveDTO> results = new ArrayList<>();
        for (Object[] o : curvesToFit) {
            String substance = (String) o[0];
            logger.info("Fit curve for substance " + substance + " and featureId " + featureResultData.getFeatureId());
            DRCInputDTO drcInput = collectCurveFitInputData(cfCtx, substance, featureResultData);
            
            ScriptExecutionRequest request = executeReceptor2CurveFit(drcInput);
            try { request.awaitOutput(); } catch (InterruptedException e) {}
            
            ScriptExecutionOutputDTO outputDTO = request.getOutput();
            if (isNotBlank(outputDTO.getOutput())) {
                logger.info("Output is " + outputDTO.getOutput());
                try {
                    OutputWrapper outputWrapper = objectMapper.readValue(outputDTO.getOutput(), OutputWrapper.class);
                    if (outputWrapper.output != null) {
                        createNewCurve(drcInput, outputWrapper.output);
                    }
                } catch (JsonProcessingException e) {
                	//TODO: Process error correctly
                	logger.error("No curve is created due to " + e.getMessage());
                }
            } else {
                logger.info("Not output is created!!");
            }
        }

        return results;
    }

    private void createNewCurve(DRCInputDTO drcInput, DRCOutputDTO drcOutput) {
        CurveDTO curveDTO = CurveDTO.builder()
                .substanceName(drcInput.getSubstance())
                .plateId(drcInput.getPlateId())
                .protocolId(drcInput.getProtocolId())
                .featureId(drcInput.getFeatureId())
                .resultSetId(drcInput.getResultSetId())
                .wells(drcInput.getWells())
                .wellConcentrations(drcInput.getConcs())
                .featureValues(drcInput.getValues())
                .fitDate(new Date())
                .version("0.0.1")
                .plotDoseData(drcOutput.dataPredict2Plot.dose)
                .plotPredictionData(drcOutput.dataPredict2Plot.prediction)
                .weights(drcOutput.weights)
                .pIC50(drcOutput.pIC50toReport)
                .pIC50StdErr(drcOutput.validpIC50.stdError)
                .eMax(isCreatable(drcOutput.rangeResults.eMax.response) ? parseFloat(drcOutput.rangeResults.eMax.response) : NaN)
                .eMin(isCreatable(drcOutput.rangeResults.eMin.response) ? parseFloat(drcOutput.rangeResults.eMin.response) : NaN)
                .eMaxConc(isCreatable(drcOutput.rangeResults.eMax.dose) ? parseFloat(drcOutput.rangeResults.eMax.dose) : NaN)
                .eMinConc(isCreatable(drcOutput.rangeResults.eMin.dose) ? parseFloat(drcOutput.rangeResults.eMin.dose) : NaN)
                .pIC20(isCreatable(drcOutput.validpIC20.estimate) ? parseFloat(drcOutput.validpIC20.estimate) : NaN)
                .pIC80(isCreatable(drcOutput.validpIC80.estimate) ? parseFloat(drcOutput.validpIC80.estimate) : NaN)
                .slope(isCreatable(drcOutput.modelCoefs.slope.estimate) ? parseFloat(drcOutput.modelCoefs.slope.estimate) : NaN)
                .bottom(isCreatable(drcOutput.modelCoefs.bottom.estimate) ? parseFloat(drcOutput.modelCoefs.bottom.estimate) : NaN)
                .top(isCreatable(drcOutput.modelCoefs.top.estimate) ? parseFloat(drcOutput.modelCoefs.top.estimate) : NaN)
                .slopeLowerCI(isCreatable(drcOutput.modelCoefs.slope.lowerCI) ? parseFloat(drcOutput.modelCoefs.slope.lowerCI) : NaN)
                .slopeUpperCI(isCreatable(drcOutput.modelCoefs.slope.upperCI) ? parseFloat(drcOutput.modelCoefs.slope.upperCI) : NaN)
                .residualVariance(isCreatable(drcOutput.residualVariance) ? parseFloat(drcOutput.residualVariance) : NaN)
                .warning(drcOutput.warning)
                .build();
        kafkaProducerService.sendCurveData(curveDTO);
    }

    private DRCInputDTO collectCurveFitInputData(CurveFittingContext ctx, String substanceName, ResultDataDTO featureResult) {
        var wells = ctx.getWells().stream()
                .filter(w -> w.getWellSubstance() != null && w.getWellSubstance().getName().equals(substanceName))
                .collect(Collectors.toList());
        var drcModelDTO = ctx.getDrcModel();

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
            accepts[i] = (wells.get(i).getStatus().getCode() >= 0 && ctx.getPlate().getValidationStatus().getCode() >= 0 && ctx.getPlate().getApprovalStatus().getCode() >= 0) ? 1 : 0;

            // Set the well feature value
            var valueIndex = WellNumberUtils.getWellNr(wells.get(i).getRow(), wells.get(i).getColumn(), ctx.getPlate().getColumns()) - 1;
            values[i] = featureResult.getValues()[valueIndex];
        }

        return DRCInputDTO.builder()
                .substance(substanceName)
                .plateId(ctx.getPlate().getId())
                .featureId(featureResult.getFeatureId())
                .wells(wellIds)
                .values(values)
                .concs(concs)
                .accepts(accepts)
                .drcModel(Optional.of(drcModelDTO))
                .build();
    }

    private ScriptExecutionRequest executeReceptor2CurveFit(DRCInputDTO inputDTO) {
        logger.info(String.format("Fitting curve for substance %s and feature ID %s", inputDTO.getSubstance(), inputDTO.getFeatureId()));

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
                "output <- NULL\n" +
                "output$pIC50toReport <- value$pIC50toReport\n" +
                "output$validpIC50 <- value$validpIC50\n" +
                "output$rangeResults$eMin <- value$rangeResults[c(\"eMin\"),]\n" +
                "output$rangeResults$eMax <- value$rangeResults[c(\"eMax\"),]\n" +
                "output$validpIC20 <- value$validpIC20[c(\"e:1:20\"),]\n" +
                "output$validpIC80 <- value$validpIC80[c(\"e:1:80\"),]\n" +
                "output$dataPredict2Plot <- value$dataPredict2Plot \n" +
                "output$weights <- value$weights\n" +
                "output$modelCoefs$Slope <- value$modelCoefs[c(\"Slope\"),]\n" +
                "output$modelCoefs$Bottom <- value$modelCoefs[c(\"Bottom\"),]\n" +
                "output$modelCoefs$Top <- value$modelCoefs[c(\"Top\"),]\n" +
                "output$modelCoefs$negLog10ED50 <- value$modelCoefs[c(\"-log10ED50\"),]\n" +
                "output$residulaVariance <- value$residulaVariance\n" +
                "output$warningFit <- value$warningFit\n";
                // TODO: Later include pIC50Location value(s)
                // "output$pIC50Location <- value$pIC50Location[1]\n" +
                // "output$pIC50LocationPrediction <- value$pIC50Location[2]\n" +
        
        return scriptExecutionService.submit(ScriptLanguage.R, script, inputVariables);
    }

    private static class OutputWrapper {

        public final DRCOutputDTO output;

        @JsonCreator
        private OutputWrapper(@JsonProperty(value = "output", required = true) DRCOutputDTO output) {
            this.output = output;
        }
    }

    private static class DRCOutputDTO {
        public String pIC50toReport;
        public ValidIC50DTO validpIC50;
        public ValidICDTO validpIC20;
        public ValidICDTO validpIC80;
        public RangeResultsDTO rangeResults;
        public DataPredict2PlotDTO dataPredict2Plot;
        public float[] weights;
        public ModelCoefsDTO modelCoefs;
        public String residualVariance;
        public String warning;

        public DRCOutputDTO(@JsonProperty(value = "pIC50toReport") String pIC50toReport,
                            @JsonProperty(value = "validpIC50") ValidIC50DTO validpIC50,
                            @JsonProperty(value = "validpIC20") ValidICDTO validpIC20,
                            @JsonProperty(value = "validpIC80") ValidICDTO validpIC80,
                            @JsonProperty(value = "rangeResults") RangeResultsDTO rangeResults,
                            @JsonProperty(value = "dataPredict2Plot") DataPredict2PlotDTO dataPredict2Plot,
                            @JsonProperty(value = "weights") float[] weights,
                            @JsonProperty(value = "modelCoefs") ModelCoefsDTO modelCoefs,
                            @JsonProperty(value = "residulaVariance") String residualVariance,
                            @JsonProperty(value = "warningFit") String warning) {
            this.pIC50toReport = pIC50toReport;
            this.validpIC50 = validpIC50;
            this.validpIC20 = validpIC20;
            this.validpIC80 = validpIC80;
            this.rangeResults = rangeResults;
            this.dataPredict2Plot = dataPredict2Plot;
            this.weights = weights;
            this.modelCoefs = modelCoefs;
            this.residualVariance = residualVariance;
            this.warning = warning;
        }
    }
    
    private static class DataPredict2PlotDTO {

        public float[] dose;
        public float[] prediction;
        public float[] lower;
        public float[] upper;

        @JsonCreator
        private DataPredict2PlotDTO(@JsonProperty(value = "dose") float[] dose,
                                    @JsonProperty(value = "Prediction") float[] prediction,
                                    @JsonProperty(value = "Lower") float[] lower,
                                    @JsonProperty(value = "Upper") float[] upper) {
            this.dose = dose;
            this.prediction = prediction;
            this.lower = lower;
            this.upper = upper;
        }
    }

    private static class ModelCoefsDTO {
        public ModelCoefDTO slope;
        public ModelCoefDTO bottom;
        public ModelCoefDTO top;
        public ModelCoefDTO negLog10ED50;

        @JsonCreator
        private ModelCoefsDTO(@JsonProperty(value = "Slope") ModelCoefDTO slope,
                              @JsonProperty(value = "Bottom") ModelCoefDTO bottom,
                              @JsonProperty(value = "Top") ModelCoefDTO top,
                              @JsonProperty(value = "negLog10ED50") ModelCoefDTO negLog10ED50) {
            this.slope = slope;
            this.bottom = bottom;
            this.top = top;
            this.negLog10ED50 = negLog10ED50;
        }
    }

    private static class ModelCoefDTO {
        public String estimate;
        public String stdError;
        public String tValue;
        public String pValue;
        public String lowerCI;
        public String upperCI;

        @JsonCreator
        public ModelCoefDTO(@JsonProperty(value = "Estimate") String estimate,
                            @JsonProperty(value = "Std. Error") String stdError,
                            @JsonProperty(value = "t-value") String tValue,
                            @JsonProperty(value = "p-value") String pValue,
                            @JsonProperty(value = "LowerCI") String lowerCI,
                            @JsonProperty(value = "upperCI") String upperCI) {
            this.estimate = estimate;
            this.stdError = stdError;
            this.tValue = tValue;
            this.pValue = pValue;
            this.lowerCI = lowerCI;
            this.upperCI = upperCI;
        }
    }

    private static class RangeResultsDTO {
        public RangeResultDTO eMax;
        public RangeResultDTO eMin;

        @JsonCreator
        public RangeResultsDTO(@JsonProperty(value = "eMax") RangeResultDTO eMax,
                               @JsonProperty(value = "eMin") RangeResultDTO eMin) {
            this.eMax = eMax;
            this.eMin = eMin;
        }
    }

    private static class RangeResultDTO {
        public String dose;
        public String response;

        @JsonCreator
        public RangeResultDTO(@JsonProperty(value = "dose") String dose,
                              @JsonProperty(value = "response") String response) {
            this.dose = dose;
            this.response = response;
        }
    }

    private static class ValidIC50DTO {
        public String estimate;
        public String stdError;
        public String lower;
        public String upper;

        @JsonCreator
        public ValidIC50DTO(@JsonProperty(value = "Estimate") String estimate,
                            @JsonProperty(value = "Std. Error") String stdError,
                            @JsonProperty(value = "LowerCI") String lower,
                            @JsonProperty(value = "upperCI") String upper) {
            this.estimate = estimate;
            this.stdError = stdError;
            this.lower = lower;
            this.upper = upper;
        }
    }
    
    private static class ValidICDTO {
        public String estimate;
        public String stdError;
        public String lower;
        public String upper;

        @JsonCreator
        public ValidICDTO(@JsonProperty(value = "Estimate") String estimate,
                          @JsonProperty(value = "Std. Error") String stdError,
                          @JsonProperty(value = "Lower") String lower,
                          @JsonProperty(value = "Upper") String upper) {
            this.estimate = estimate;
            this.stdError = stdError;
            this.lower = lower;
            this.upper = upper;
        }
    }

}
