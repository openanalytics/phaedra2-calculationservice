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

import static java.lang.Float.NaN;
import static java.lang.Float.parseFloat;
import static java.util.stream.IntStream.range;
import static org.apache.commons.lang3.math.NumberUtils.isCreatable;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;

import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.math3.util.Precision;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

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
import eu.openanalytics.phaedra.calculationservice.execution.CalculationContext;
import eu.openanalytics.phaedra.calculationservice.execution.progress.CalculationStage;
import eu.openanalytics.phaedra.calculationservice.execution.progress.CalculationStateEventCode;
import eu.openanalytics.phaedra.calculationservice.execution.script.ScriptExecutionRequest;
import eu.openanalytics.phaedra.calculationservice.execution.script.ScriptExecutionService;
import eu.openanalytics.phaedra.calculationservice.service.KafkaProducerService;
import eu.openanalytics.phaedra.plateservice.client.PlateServiceClient;
import eu.openanalytics.phaedra.plateservice.dto.PlateDTO;
import eu.openanalytics.phaedra.plateservice.dto.WellDTO;
import eu.openanalytics.phaedra.protocolservice.client.ProtocolServiceClient;
import eu.openanalytics.phaedra.protocolservice.dto.DRCModelDTO;
import eu.openanalytics.phaedra.protocolservice.dto.FeatureDTO;
import eu.openanalytics.phaedra.protocolservice.record.InputParameter;
import eu.openanalytics.phaedra.resultdataservice.dto.ResultDataDTO;
import eu.openanalytics.phaedra.scriptengine.dto.ResponseStatusCode;
import eu.openanalytics.phaedra.scriptengine.dto.ScriptExecutionOutputDTO;
import eu.openanalytics.phaedra.util.WellNumberUtils;

@Service
public class CurveFittingExecutorService {

    private final PlateServiceClient plateServiceClient;
    private final ProtocolServiceClient protocolServiceClient;

    private final KafkaProducerService kafkaProducerService;
    private final ScriptExecutionService scriptExecutionService;

    private final ObjectMapper objectMapper;
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
    }

    public void execute(CalculationContext ctx, FeatureDTO feature) {
    	List<String> substanceNames = ctx.getWells().stream()
    			.filter(w -> w.getWellSubstance() != null)
    			.map(w -> w.getWellSubstance().getName())
    			.distinct().toList();

    	if (feature.getDrcModel() == null || StringUtils.isBlank(feature.getDrcModel().getName()) || substanceNames.isEmpty()) {
    		// There is nothing to fit for this feature.
    		ctx.getStateTracker().skipStage(feature.getId(), CalculationStage.FeatureCurveFit);
    		return;
    	}

    	ResultDataDTO resultData = ctx.getFeatureResults().get(feature.getId());
    	if (resultData == null || resultData.getValues() == null || resultData.getValues().length == 0) {
    		ctx.getStateTracker().failStage(feature.getId(), CalculationStage.FeatureCurveFit,
    				String.format("Cannot fit curve: no result data available for feature %s", feature), feature);
    		return;
    	}

    	ctx.getStateTracker().startStage(feature.getId(), CalculationStage.FeatureCurveFit, substanceNames.size());
    	for (String substance: substanceNames) {
            DRCInputDTO drcInput = collectCurveFitInputData(ctx.getPlate(), ctx.getWells(), resultData, feature, substance);
            ScriptExecutionRequest request = executeReceptor2CurveFit(drcInput);
            ctx.getStateTracker().trackScriptExecution(feature.getId(), CalculationStage.FeatureCurveFit, substance, request);
        }

        ctx.getStateTracker().addEventListener(CalculationStage.FeatureCurveFit, CalculationStateEventCode.ScriptOutputAvailable, feature.getId(), requests -> {
        	requests.entrySet().stream().forEach(req -> {
				if (StringUtils.isBlank(req.getValue().getOutput().getOutput())) {
					logger.info("No output is created!!");
				} else {
					DRCInputDTO drcInput = collectCurveFitInputData(ctx.getPlate(), ctx.getWells(), resultData, feature, req.getKey());
					DRCOutputDTO drcOutput = collectCurveFitOutputData(req.getValue().getOutput());
					if (drcOutput != null) createNewCurve(drcInput, drcOutput);
				}
			});
        	//TODO CurveDataService should emit an event when the curve is saved, to which StateTracker can respond
        	ctx.getStateTracker().skipStage(feature.getId(), CalculationStage.FeatureCurveFit);
        });

        ctx.getStateTracker().addEventListener(CalculationStage.FeatureCurveFit, CalculationStateEventCode.Error, feature.getId(), requests -> {
        	requests.values().stream().map(req -> req.getOutput())
				.filter(o -> o.getStatusCode() != ResponseStatusCode.SUCCESS)
				.forEach(o -> ctx.getErrorCollector().addError(String.format("Curve fit failed with status %s", o.getStatusCode()), o, feature));
        });
    }

    public void execute(CurveFittingRequestDTO request) {
    	try {
    		FeatureDTO feature = protocolServiceClient.getFeature(request.getFeatureId());
    		if (feature.getDrcModel() == null) {
    			// There is no model to fit for this feature.
    			logger.warn(String.format("Aborting curve fit: no fit model found on feature %d", request.getFeatureId()));
    			return;
    		}

    		ResultDataDTO resultData = request.getFeatureResultData();
        	if (resultData == null || resultData.getValues() == null || resultData.getValues().length == 0) {
        		// There is no data to fit for this feature.
        		logger.warn(String.format("Aborting curve fit: no data provided for plate %d, feature %d", request.getPlateId(), request.getFeatureId()));
        		return;
        	}

        	PlateDTO plate = plateServiceClient.getPlate(request.getPlateId());
        	List<WellDTO> wells = plateServiceClient.getWells(plate.getId());

    		List<String> substanceNames = wells.stream().filter(w -> w.getWellSubstance() != null).map(w -> w.getWellSubstance().getName()).distinct().toList();
        	for (String substance: substanceNames) {
        		DRCInputDTO drcInput = collectCurveFitInputData(plate, wells, resultData, feature, substance);
        		ScriptExecutionRequest scriptRequest = executeReceptor2CurveFit(drcInput);
        		scriptRequest.addCallback(output -> {
        			DRCOutputDTO drcOutput = collectCurveFitOutputData(output);
					if (drcOutput != null) createNewCurve(drcInput, drcOutput);
        		});
        	}
    	} catch (Exception e) {
    		logger.error(String.format("Curve fit failed on plate %d, feature %d", request.getPlateId(), request.getFeatureId()), e);
    	}
    }

    private void createNewCurve(DRCInputDTO drcInput, DRCOutputDTO drcOutput) {
        logger.info("Create new curve for substance %s and feature %s (%d)", drcInput.getSubstance(), drcInput.getFeature().getName(), drcInput.getFeature().getId());
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

    private DRCInputDTO collectCurveFitInputData(PlateDTO plate, List<WellDTO> plateWells, ResultDataDTO resultData, FeatureDTO feature, String substanceName) {
        var wells = plateWells.stream()
                .filter(w -> w.getWellSubstance() != null && substanceName.equals(w.getWellSubstance().getName()))
                .toList();
        var drcModelDTO = feature.getDrcModel();

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
            accepts[i] = (wells.get(i).getStatus().getCode() >= 0 && plate.getValidationStatus().getCode() >= 0 && plate.getApprovalStatus().getCode() >= 0) ? 1 : 0;

            // Set the well feature value
            var valueIndex = WellNumberUtils.getWellNr(wells.get(i).getRow(), wells.get(i).getColumn(), plate.getColumns()) - 1;
            values[i] = resultData.getValues()[valueIndex];
        });

        return DRCInputDTO.builder()
                .substance(substanceName)
                .plateId(plate.getId())
                .feature(feature)
                .protocolId(feature.getProtocolId())
                .resultSetId(resultData.getResultSetId())
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
        inputVariables.put("substance", inputDTO.getSubstance());
        inputVariables.put("doses", inputDTO.getConcs());
        inputVariables.put("responses", inputDTO.getValues());
        inputVariables.put("accepts", inputDTO.getAccepts());


        if (inputDTO.getDrcModel().isPresent()) {
            DRCModelDTO drcModel = inputDTO.getDrcModel().get();
            logger.info("Input DRCModel: " + drcModel);
            inputVariables.put("fixedBottom", drcModel.getInputParameters().stream().filter(inParam -> inParam.name().equals("fixedBottom")).findFirst().orElseGet(() -> new InputParameter("fixedBottom", "NA")).value());
            inputVariables.put("fixedTop", drcModel.getInputParameters().stream().filter(inParam -> inParam.name().equals("fixedTop")).findFirst().orElseGet(() -> new InputParameter("fixedTop", "NA")).value());
            inputVariables.put("fixedSlope", drcModel.getInputParameters().stream().filter(inParam -> inParam.name().equals("fixedSlope")).findFirst().orElseGet(() -> new InputParameter("fixedSlope", "NA")).value());
            inputVariables.put("confLevel", drcModel.getInputParameters().stream().filter(inParam -> inParam.name().equals("confLevel")).findFirst().orElseGet(() -> new InputParameter("confLevel", "0.95")).value());
            inputVariables.put("robustMethod", drcModel.getInputParameters().stream().filter(inParam -> inParam.name().equals("robustMethod")).findFirst().orElseGet(() -> new InputParameter("robustMethod", "mean")).value());
            inputVariables.put("slopeType", drcModel.getInputParameters().stream().filter(inParam -> inParam.name().equals("slopeType")).findFirst().orElseGet(() -> new InputParameter("slopeType", "ascending")).value());
            inputVariables.put("responseName", inputDTO.getFeature().getName());
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
