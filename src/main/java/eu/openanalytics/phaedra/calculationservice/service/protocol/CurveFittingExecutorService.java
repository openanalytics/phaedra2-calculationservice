/**
 * Phaedra II
 *
 * Copyright (C) 2016-2024 Open Analytics
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
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.openanalytics.curvedataservice.dto.CurveDTO;
import eu.openanalytics.curvedataservice.dto.CurvePropertyDTO;
import eu.openanalytics.phaedra.calculationservice.dto.CurveFittingRequestDTO;
import eu.openanalytics.phaedra.calculationservice.dto.DRCInputDTO;
import eu.openanalytics.phaedra.calculationservice.enumeration.ScriptLanguage;
import eu.openanalytics.phaedra.calculationservice.exception.NoDRCModelDefinedForFeature;
import eu.openanalytics.phaedra.calculationservice.model.CurveFittingContext;
import eu.openanalytics.phaedra.calculationservice.service.KafkaProducerService;
import eu.openanalytics.phaedra.calculationservice.service.script.ScriptExecutionRequest;
import eu.openanalytics.phaedra.calculationservice.service.script.ScriptExecutionService;
import eu.openanalytics.phaedra.plateservice.client.PlateServiceClient;
import eu.openanalytics.phaedra.plateservice.client.exception.PlateUnresolvableException;
import eu.openanalytics.phaedra.protocolservice.client.ProtocolServiceClient;
import eu.openanalytics.phaedra.protocolservice.client.exception.FeatureUnresolvableException;
import eu.openanalytics.phaedra.protocolservice.dto.DRCModelDTO;
import eu.openanalytics.phaedra.protocolservice.record.InputParameter;
import eu.openanalytics.phaedra.resultdataservice.dto.ResultDataDTO;
import eu.openanalytics.phaedra.scriptengine.dto.ScriptExecutionOutputDTO;
import eu.openanalytics.phaedra.util.WellNumberUtils;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.math3.util.Precision;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static java.lang.Float.NaN;
import static java.lang.Float.parseFloat;
import static java.util.stream.IntStream.range;
import static org.apache.commons.lang3.StringUtils.isNotBlank;
import static org.apache.commons.lang3.math.NumberUtils.isCreatable;

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

    public record CurveFittingExecution(Future<List<CurveDTO>> curves) {};

    public CurveFittingExecution execute(CurveFittingRequestDTO curveFittingRequestDTO) {
        return new CurveFittingExecution(executorService.submit(() -> {
            try {
                return executeCurveFit(curveFittingRequestDTO);
            } catch (Throwable ex) {
                // print the stack strace. Since the future may never be awaited, we may not see the error otherwise
                ex.printStackTrace();
                throw ex;
            }
        }));
    }

    private List<CurveDTO> executeCurveFit(CurveFittingRequestDTO curveFittingRequestDTO) throws PlateUnresolvableException, FeatureUnresolvableException {
        var plate  = plateServiceClient.getPlate(curveFittingRequestDTO.getPlateId());
        var wells = plateServiceClient.getWells(curveFittingRequestDTO.getPlateId());
        var substances = plateServiceClient.getWellSubstances(curveFittingRequestDTO.getPlateId());

        if (curveFittingRequestDTO.getFeatureResultData() == null) {
            logger.info("Feature result data is null!!");
            return null;
        }

        logger.info("Get feature by featureId: " + curveFittingRequestDTO.getFeatureId());
        var feature = protocolServiceClient.getFeature(curveFittingRequestDTO.getFeatureId());
        if (feature.getDrcModel() == null) {
            logger.info("No drcModel found featureId: " + curveFittingRequestDTO.getFeatureId());
            return null;
        }

//        var wellSubstances = substances.stream().filter(s -> "COMPOUND".equalsIgnoreCase(s.getType())).toList();
        logger.info("Nr of substances found for plate " + plate.getId() + ": " + substances.size());
        var wellSubstancesUnique = substances.stream().map(s -> s.getName()).toList().stream().distinct().toList();
        logger.info("Nr of unique substances found for plate " + plate.getId() + ": " + wellSubstancesUnique.size());

        if (CollectionUtils.isEmpty(wellSubstancesUnique))
            return null; //TODO: Return a proper error

        var cfCtx = CurveFittingContext.newInstance(plate, wells, substances, wellSubstancesUnique, feature, feature.getDrcModel());

        List<CurveDTO> results = new ArrayList<>();
        for (String substance: wellSubstancesUnique) {
            logger.info("Fit curve for substance " + substance + " and featureId " + curveFittingRequestDTO.getFeatureId());
            DRCInputDTO drcInput = collectCurveFitInputData(cfCtx, substance, curveFittingRequestDTO.getFeatureResultData());

            ScriptExecutionRequest request = executeReceptor2CurveFit(drcInput);
            try { request.awaitOutput(); } catch (InterruptedException e) {}

            ScriptExecutionOutputDTO outputDTO = request.getOutput();
            if (isNotBlank(outputDTO.getOutput())) {
                logger.info("Output is " + outputDTO.getOutput());
                DRCOutputDTO drcOutput = collectCurveFitOutputData(outputDTO);
                if (drcOutput != null)
                    createNewCurve(drcInput, drcOutput);
            } else {
                logger.info("Not output is created!!");
            }
        }

        return results;
    }

    private void createNewCurve(DRCInputDTO drcInput, DRCOutputDTO drcOutput) {
        List<CurvePropertyDTO> curvePropertieDTOs = new ArrayList<>();
        if (isCreatable(drcOutput.pIC50toReport))
            curvePropertieDTOs.add(CurvePropertyDTO.builder().name("pIC50").numericValue(parseFloat(drcOutput.pIC50toReport)).build());
        else {
            if (drcOutput.pIC50toReport.startsWith("<") || drcOutput.pIC50toReport.startsWith(">")) {
                curvePropertieDTOs.add(CurvePropertyDTO.builder().name("pIC50").numericValue(parseFloat(drcOutput.pIC50toReport.substring(1).trim())).build());
                curvePropertieDTOs.add(CurvePropertyDTO.builder().name("pIC50 Censor").stringValue(drcOutput.pIC50toReport.substring(0,1)).build());
            }
        }

        if (isCreatable(drcOutput.validpIC50.stdError))
            curvePropertieDTOs.add(CurvePropertyDTO.builder().name("pIC50 StdErr").numericValue(parseFloat(drcOutput.validpIC50.stdError)).build());
        else
            curvePropertieDTOs.add(CurvePropertyDTO.builder().name("pIC50 StdErr").numericValue(NaN).build());

        if (drcOutput.modelCoefs != null) {
            curvePropertieDTOs.add(CurvePropertyDTO.builder().name("Bottom").numericValue(isCreatable(drcOutput.modelCoefs.bottom.estimate) ? parseFloat(drcOutput.modelCoefs.bottom.estimate) : NaN).build());
            curvePropertieDTOs.add(CurvePropertyDTO.builder().name("Top").numericValue(isCreatable(drcOutput.modelCoefs.top.estimate) ? parseFloat(drcOutput.modelCoefs.top.estimate) : NaN).build());
            curvePropertieDTOs.add(CurvePropertyDTO.builder().name("Slope").numericValue(isCreatable(drcOutput.modelCoefs.slope.estimate) ? parseFloat(drcOutput.modelCoefs.slope.estimate) : NaN).build());
            curvePropertieDTOs.add(CurvePropertyDTO.builder().name("Slope Lower CI").numericValue(isCreatable(drcOutput.modelCoefs.slope.lowerCI) ? parseFloat(drcOutput.modelCoefs.slope.lowerCI) : NaN).build());
            curvePropertieDTOs.add(CurvePropertyDTO.builder().name("Slope Upper CI").numericValue(isCreatable(drcOutput.modelCoefs.slope.upperCI) ? parseFloat(drcOutput.modelCoefs.slope.upperCI) : NaN).build());
        } else {
            curvePropertieDTOs.add(CurvePropertyDTO.builder().name("Bottom").numericValue(NaN).build());
            curvePropertieDTOs.add(CurvePropertyDTO.builder().name("Top").numericValue(NaN).build());
            curvePropertieDTOs.add(CurvePropertyDTO.builder().name("Slope").numericValue(NaN).build());
            curvePropertieDTOs.add(CurvePropertyDTO.builder().name("Slope Lower CI").numericValue(NaN).build());
            curvePropertieDTOs.add(CurvePropertyDTO.builder().name("Slope Upper CI").numericValue(NaN).build());
        }

        if (drcOutput.rangeResults != null) {
            curvePropertieDTOs.add(CurvePropertyDTO.builder().name("eMin").numericValue(isCreatable(drcOutput.rangeResults.eMin.response) ? parseFloat(drcOutput.rangeResults.eMin.response) : NaN).build());
            curvePropertieDTOs.add(CurvePropertyDTO.builder().name("eMin Conc").numericValue(isCreatable(drcOutput.rangeResults.eMin.dose) ? parseFloat(drcOutput.rangeResults.eMin.dose) : NaN).build());
            curvePropertieDTOs.add(CurvePropertyDTO.builder().name("eMax").numericValue(isCreatable(drcOutput.rangeResults.eMax.response) ? parseFloat(drcOutput.rangeResults.eMax.response) : NaN).build());
            curvePropertieDTOs.add(CurvePropertyDTO.builder().name("eMax Conc").numericValue(isCreatable(drcOutput.rangeResults.eMax.dose) ? parseFloat(drcOutput.rangeResults.eMax.dose) : NaN).build());
        } else {
            curvePropertieDTOs.add(CurvePropertyDTO.builder().name("eMin").numericValue(NaN).build());
            curvePropertieDTOs.add(CurvePropertyDTO.builder().name("eMin Conc").numericValue(NaN).build());
            curvePropertieDTOs.add(CurvePropertyDTO.builder().name("eMax").numericValue(NaN).build());
            curvePropertieDTOs.add(CurvePropertyDTO.builder().name("eMax Conc").numericValue(NaN).build());
        }

        if (drcOutput.validpIC20 != null)
            curvePropertieDTOs.add(CurvePropertyDTO.builder().name("pIC20").numericValue(isCreatable(drcOutput.validpIC20.estimate) ? parseFloat(drcOutput.validpIC20.estimate) : NaN).build());
        else
            curvePropertieDTOs.add(CurvePropertyDTO.builder().name("pIC20").numericValue(NaN).build());


        if (drcOutput.validpIC80 != null)
            curvePropertieDTOs.add(CurvePropertyDTO.builder().name("pIC80").numericValue(isCreatable(drcOutput.validpIC80.estimate) ? parseFloat(drcOutput.validpIC80.estimate) : NaN).build());
        else
            curvePropertieDTOs.add(CurvePropertyDTO.builder().name("pIC80").numericValue(NaN).build());

        curvePropertieDTOs.add(CurvePropertyDTO.builder().name("Residual Variance").numericValue(isCreatable(drcOutput.residualVariance) ? parseFloat(drcOutput.residualVariance) : NaN).build());
        curvePropertieDTOs.add(CurvePropertyDTO.builder().name("Warning").stringValue(drcOutput.warning).build());

        CurveDTO curveDTO = CurveDTO.builder()
                .substanceName(drcInput.getSubstance())
                .plateId(drcInput.getPlateId())
                .protocolId(drcInput.getProtocolId())
                .featureId(drcInput.getFeature().getId())
                .resultSetId(drcInput.getResultSetId())
                .wells(drcInput.getWells())
                .wellConcentrations(drcInput.getConcs())
                .featureValues(drcInput.getValues())
                .fitDate(new Date())
                .version("0.0.1")
                .plotDoseData(drcOutput.dataPredict2Plot.dose)
                .plotPredictionData(drcOutput.dataPredict2Plot.prediction)
                .weights(drcOutput.weights)
                .curveProperties(curvePropertieDTOs)
                .build();
        kafkaProducerService.sendCurveData(curveDTO);
    }

    private DRCInputDTO collectCurveFitInputData(CurveFittingContext ctx, String substanceName, ResultDataDTO featureResult) {
        var wells = ctx.getWells().stream()
                .filter(w -> w.getWellSubstance() != null && w.getWellSubstance().getName().equals(substanceName))
                .toList();
        var drcModelDTO = ctx.getDrcModel();

        long[] wellIds = new long[wells.size()];
        float[] concs = new float[wells.size()];
        float[] accepts = new float[wells.size()];
        float[] values = new float[wells.size()];

        range(0, wells.size()).forEach(i -> {
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
        });

        return DRCInputDTO.builder()
                .substance(substanceName)
                .plateId(ctx.getPlate().getId())
                .feature(ctx.getFeature())
                .wells(wellIds)
                .values(values)
                .concs(concs)
                .accepts(accepts)
                .drcModel(Optional.of(drcModelDTO))
                .build();
    }

    private DRCOutputDTO collectCurveFitOutputData(ScriptExecutionOutputDTO outputDTO) {
        try {
            OutputWrapper outputWrapper = objectMapper.readValue(outputDTO.getOutput(), OutputWrapper.class);
            return outputWrapper.output;
        } catch (JsonProcessingException e) {
            return null;
        }
    }

    private ScriptExecutionRequest executeReceptor2CurveFit(DRCInputDTO inputDTO) {
        logger.info(String.format("Fitting curve for substance %s and feature ID %s", inputDTO.getSubstance(), inputDTO.getFeature().getId()));

        var inputVariables = new HashMap<String, Object>();
        inputVariables.put("doses", inputDTO.getConcs());
        inputVariables.put("responses", inputDTO.getValues());
        inputVariables.put("accepts", inputDTO.getAccepts());


        if (inputDTO.getDrcModel().isPresent()) {
            DRCModelDTO drcModel = inputDTO.getDrcModel().get();
            logger.info("Input DRCModel: " + drcModel);
//            inputVariables.put("fixedBottom", drcModel.getInputParameters().get("fixedBottom"));
            inputVariables.put("fixedBottom", drcModel.getInputParameters().stream().filter(inParam -> inParam.equals("fixedBottom")).findFirst().get().value());
//            inputVariables.put("fixedTop", drcModel.getInputParameters().get("fixedTop"));
            inputVariables.put("fixedTop", drcModel.getInputParameters().stream().filter(inParam -> inParam.equals("fixedTop")).findFirst().get().value());
//            inputVariables.put("fixedSlope", drcModel.getInputParameters().get("fixedSlope"));
            inputVariables.put("fixedSlope", drcModel.getInputParameters().stream().filter(inParam -> inParam.equals("fixedSlope")).findFirst().get().value());
//            inputVariables.put("confLevel", drcModel.getInputParameters().get("confLevel"));
            inputVariables.put("confLevel", drcModel.getInputParameters().stream().filter(inParam -> inParam.equals("confLevel")).findFirst().get().value());
//            inputVariables.put("robustMethod", drcModel.getInputParameters().get("robustMethod") != null ? drcModel.getInputParameters().get("robustMethod") : "mean");
            inputVariables.put("robustMethod", drcModel.getInputParameters().stream().filter(inParam -> inParam.equals("robustMethod")).findFirst().orElseGet(() -> new InputParameter("robustMethod", "mean")).value());
            inputVariables.put("responseName", inputDTO.getFeature().getName());
//            inputVariables.put("slopeType", drcModel.getInputParameters().get("slopeType") != null ? drcModel.getInputParameters().get("slopeType") : "ascending");
            inputVariables.put("slopeType", drcModel.getInputParameters().stream().filter(inParam -> inParam.equals("slopeType")).findFirst().orElseGet(() -> new InputParameter("slopeType", "ascending")).value());
        } else {
            throw new NoDRCModelDefinedForFeature("No DRCModel defined for feature %s (%d)", inputDTO.getFeature().getName(), inputDTO.getFeature().getId());
        }

        var script = "library(receptor2)\n" +
                "\n" +
                "dose <- input$doses\n" +
                "response <- input$responses\n" +
                "accept <- input$accepts\n" +
                "if (is.null(input$fixedBottom) == TRUE) fixedBottom <- NA else fixedBottom <- as.numeric(input$fixedBottom)\n" +
                "if (is.null(input$fixedTop) == TRUE) fixedTop <- NA else fixedTop <- as.numeric(input$fixedTop)\n" +
                "if (is.null(input$fixedSlope) == TRUE) fixedSlope <- NA else fixedSlope <- as.numeric(input$fixedSlope)\n" +
                "if (is.null(input$confLevel) == TRUE) confLevel <- 0.95 else confLevel <- as.numeric(input$confLevel)\n" +
                "robustMethod <- input$robustMethod\n" +
                "responseName <- input$responseName\n" +
                "slopeType <- input$slopeType\n" +
                "\n" +
                "value <- fittingLogisticModel(\n" +
                "\tinputData = data.frame(dose, response),\n" +
                "\taccept = accept,\n" +
                "\tfixedBottom = fixedBottom,\n" +
                "\tfixedTop = fixedTop,\n" +
                "\tfixedSlope = fixedSlope,\n" +
                "\tconfLevel = confLevel,\n" +
                "\trobustMethod = robustMethod,\n" +
                "\tresponseName = responseName,\n" +
                "\tslope = slopeType)\n" +
                "\n" +
                "output <- NULL\n" +
                "output$pIC50toReport <- value$pIC50toReport\n" +
                "output$validpIC50 <- value$validpIC50\n" +
                "output$rangeResults$eMin <- value$rangeResults[c(\"eMin\"),]\n" +
                "output$rangeResults$eMax <- value$rangeResults[c(\"eMax\"),]\n" +
                "output$dataPredict2Plot <- value$dataPredict2Plot \n" +
                "output$dataPredict2Plot$dose <- -value$dataPredict2Plot$dose / 2.303 \n" +
                "output$weights <- value$weights\n" +
                "output$modelCoefs$Slope <- value$modelCoefs[c(\"Slope\"),]\n" +
                "output$modelCoefs$Bottom <- value$modelCoefs[c(\"Bottom\"),]\n" +
                "output$modelCoefs$Top <- value$modelCoefs[c(\"Top\"),]\n" +
                "output$modelCoefs$negLog10ED50 <- value$modelCoefs[c(\"-log10ED50\"),]\n" +
                "output$residulaVariance <- value$residulaVariance\n" +
                "output$warningFit <- value$warningFit\n";
//                 TODO: Later include pIC50Location value(s)
//                 "output$pIC50Location <- value$pIC50Location[1]\n" +
//                 "output$pIC50LocationPrediction <- value$pIC50Location[2]\n" +
//                 "output$validpIC20 <- value$validpIC20[c(\"e:1:20\"),]\n" +
//                 "output$validpIC80 <- value$validpIC80[c(\"e:1:80\"),]\n" +

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
