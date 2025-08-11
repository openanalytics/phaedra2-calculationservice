/**
 * Phaedra II
 *
 * Copyright (C) 2016-2025 Open Analytics
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

import static java.util.stream.IntStream.range;
import static org.apache.commons.lang3.math.NumberUtils.isCreatable;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.openanalytics.phaedra.calculationservice.dto.CurveFittingRequestDTO;
import eu.openanalytics.phaedra.calculationservice.dto.DRCInputDTO;
import eu.openanalytics.phaedra.calculationservice.dto.ScriptExecutionOutputDTO;
import eu.openanalytics.phaedra.calculationservice.enumeration.FormulaCategory;
import eu.openanalytics.phaedra.calculationservice.enumeration.ResponseStatusCode;
import eu.openanalytics.phaedra.calculationservice.enumeration.ScriptLanguage;
import eu.openanalytics.phaedra.calculationservice.exception.FormulaNotFoundException;
import eu.openanalytics.phaedra.calculationservice.exception.NoDRCModelDefinedForFeature;
import eu.openanalytics.phaedra.calculationservice.execution.CalculationContext;
import eu.openanalytics.phaedra.calculationservice.execution.progress.CalculationStage;
import eu.openanalytics.phaedra.calculationservice.execution.progress.CalculationStateEventCode;
import eu.openanalytics.phaedra.calculationservice.execution.script.ScriptExecutionRequest;
import eu.openanalytics.phaedra.calculationservice.execution.script.ScriptExecutionService;
import eu.openanalytics.phaedra.calculationservice.service.FormulaService;
import eu.openanalytics.phaedra.calculationservice.service.KafkaProducerService;
import eu.openanalytics.phaedra.plateservice.client.PlateServiceClient;
import eu.openanalytics.phaedra.plateservice.dto.PlateDTO;
import eu.openanalytics.phaedra.plateservice.dto.WellDTO;
import eu.openanalytics.phaedra.protocolservice.client.ProtocolServiceClient;
import eu.openanalytics.phaedra.protocolservice.dto.DRCModelDTO;
import eu.openanalytics.phaedra.protocolservice.dto.FeatureDTO;
import eu.openanalytics.phaedra.protocolservice.record.InputParameter;
import eu.openanalytics.phaedra.resultdataservice.dto.CurveDTO;
import eu.openanalytics.phaedra.resultdataservice.dto.CurveOutputParamDTO;
import eu.openanalytics.phaedra.resultdataservice.dto.ResultDataDTO;
import eu.openanalytics.phaedra.util.WellNumberUtils;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class CurveFittingExecutorService {

  private final PlateServiceClient plateServiceClient;
  private final ProtocolServiceClient protocolServiceClient;

  private final KafkaProducerService kafkaProducerService;
  private final ScriptExecutionService scriptExecutionService;

  private final ObjectMapper objectMapper;
  private final Logger logger = LoggerFactory.getLogger(getClass());
  private final FormulaService formulaService;

  public CurveFittingExecutorService(
      PlateServiceClient plateServiceClient,
      ProtocolServiceClient protocolServiceClient,
      KafkaProducerService kafkaProducerService,
      ScriptExecutionService scriptExecutionService,
      ObjectMapper objectMapper, FormulaService formulaService) {

    this.plateServiceClient = plateServiceClient;
    this.protocolServiceClient = protocolServiceClient;

    this.kafkaProducerService = kafkaProducerService;
    this.scriptExecutionService = scriptExecutionService;

    this.objectMapper = objectMapper;
    this.formulaService = formulaService;
  }

  public void execute(CalculationContext ctx, FeatureDTO feature) {
    List<String> substanceNames = ctx.getWells().stream()
        .filter(w -> w.getWellSubstance() != null && w.getWellSubstance().getName() != null)
        .map(w -> w.getWellSubstance().getName())
        .distinct().toList();

    if (feature.getDrcModel() == null
        || StringUtils.isBlank(feature.getDrcModel().getName())
        || substanceNames.isEmpty()) {
      // There is nothing to fit for this feature.
      ctx.getStateTracker().skipStage(feature.getId(), CalculationStage.FeatureCurveFit);
      return;
    }

    ResultDataDTO resultData = ctx.getFeatureResults().get(feature.getId());
    if (resultData == null
        || resultData.getValues() == null
        || resultData.getValues().length == 0) {
      ctx.getStateTracker().failStage(feature.getId(), CalculationStage.FeatureCurveFit,
          String.format("Cannot fit curve: no result data available for feature %s", feature),
          feature);
      return;
    }

    ctx.getStateTracker()
        .startStage(feature.getId(), CalculationStage.FeatureCurveFit, substanceNames.size());
    for (String substance : substanceNames) {
      DRCInputDTO drcInput = collectCurveFitInputData(ctx.getPlate(), ctx.getWells(), resultData,
          feature, substance);
      ScriptExecutionRequest request = executeCurveFittingModel(drcInput);
      ctx.getStateTracker()
          .trackScriptExecution(feature.getId(), CalculationStage.FeatureCurveFit, substance,
              request);
    }

    ctx.getStateTracker().addEventListener(CalculationStage.FeatureCurveFit,
        CalculationStateEventCode.ScriptOutputAvailable,
        feature.getId(), requests -> {
          requests.entrySet().stream().forEach(req -> {
            if (StringUtils.isBlank(req.getValue().getOutput().getOutput())) {
              logger.warn("Curve fit response without output: " + req.getValue().getOutput()
                  .getStatusMessage());
            } else {
              try {
                DRCInputDTO drcInput = collectCurveFitInputData(ctx.getPlate(), ctx.getWells(),
                    resultData, feature, req.getKey());
                Map<String, Object> drcOutput = collectCurveFitOutputData(req.getValue().getOutput());
                createNewCurve(drcInput, drcOutput);
              } catch (RuntimeException e) {
                logger.error("Failed to process curve fit output", e);
              }
            }
          });
          //TODO CurveDataService should emit an event when the curve is saved, to which StateTracker can respond
          ctx.getStateTracker().skipStage(feature.getId(), CalculationStage.FeatureCurveFit);
        });

    ctx.getStateTracker()
        .addEventListener(CalculationStage.FeatureCurveFit, CalculationStateEventCode.Error,
            feature.getId(), requests -> {
              requests.values().stream().map(req -> req.getOutput())
                  .filter(o -> o.getStatusCode() != ResponseStatusCode.SUCCESS)
                  .forEach(o -> ctx.getErrorCollector().addError(
                      String.format("Curve fit failed with status %s", o.getStatusCode()), o,
                      feature));
            });
  }

  public void execute(CurveFittingRequestDTO request) {
    try {
      FeatureDTO feature = protocolServiceClient.getFeature(request.getFeatureId());
      if (feature.getDrcModel() == null) {
        // There is no model to fit for this feature.
        logger.warn(String.format("Aborting curve fit: no fit model found on feature %d",
            request.getFeatureId()));
        return;
      }

      ResultDataDTO resultData = request.getFeatureResultData();
      if (resultData == null || resultData.getValues() == null
          || resultData.getValues().length == 0) {
        // There is no data to fit for this feature.
        logger.warn(String.format("Aborting curve fit: no data provided for plate %d, feature %d",
            request.getPlateId(), request.getFeatureId()));
        return;
      }

      PlateDTO plate = plateServiceClient.getPlate(request.getPlateId());
      List<WellDTO> wells = plateServiceClient.getWells(plate.getId());

      List<String> substanceNames = wells.stream()
          .filter(w -> w.getWellSubstance() != null && w.getWellSubstance().getName() != null)
          .map(w -> w.getWellSubstance().getName())
          .distinct().toList();

      for (String substanceName : substanceNames) {
        DRCInputDTO drcInput = collectCurveFitInputData(plate, wells, resultData, feature,
            substanceName);
        ScriptExecutionRequest scriptRequest = executeCurveFittingModel(drcInput);
        scriptRequest.addCallback(output -> {
          try {
            Map<String, Object> drcOutput = collectCurveFitOutputData(output);
            createNewCurve(drcInput, drcOutput);
          } catch (RuntimeException e) {
            logger.error("Failed to process curve fit output", e);
          }
        });
      }
    } catch (Exception e) {
      logger.error(String.format("Curve fit failed on plate %d, feature %d", request.getPlateId(),
          request.getFeatureId()), e);
    }
  }

  private void createNewCurve(DRCInputDTO drcInput, Map<String, Object> drcOutput) {
    logger.info("Create new curve for substance %s and feature %s (%d)", drcInput.getSubstance(),
        drcInput.getFeature().getName(), drcInput.getFeature().getId());
    List<CurveOutputParamDTO> curvePropertieDTOs = new ArrayList<>();
    for (String key : drcOutput.keySet()) {
      if (drcOutput.get(key) != null)
        if (drcOutput.get(key) instanceof String) {
          String value = (String) drcOutput.get(key);
          curvePropertieDTOs
              .add(CurveOutputParamDTO.builder()
                  .name(key)
                  .stringValue(value)
                  .build());
        } else if (drcOutput.get(key) instanceof Number) {
          Number value = (Number) drcOutput.get(key);
          curvePropertieDTOs
              .add(CurveOutputParamDTO.builder()
                  .name(key)
                  .numericValue(value.floatValue())
                  .build());
        }
    }

    float[] plotResponses = new float[0];
    if (drcOutput.containsKey("plotPredictionResponse")) {
      ArrayList<Number> values = (ArrayList<Number>) drcOutput.get("plotPredictionResponse");
      plotResponses = new float[values.size()];
      for (int i = 0; i < values.size(); i++) {
        plotResponses[i] = values.get(i).floatValue();
      }
    }
    float[] plotDoses = new float[0];
    if (drcOutput.containsKey("plotPredictionDose")) {
      ArrayList<Number> values = (ArrayList<Number>) drcOutput.get("plotPredictionDose");
      plotDoses = new float[values.size()];
      for (int i = 0; i < values.size(); i++) {
        plotDoses[i] = values.get(i).floatValue();
      }
    }
    float[] weights = new float[0];
    if (drcOutput.containsKey("weights")) {
      ArrayList<Number> values = (ArrayList<Number>) drcOutput.get("weights");
      weights = new float[values.size()];
      for (int i = 0; i < values.size(); i++) {
        weights[i] = values.get(i).floatValue();
      }
    }

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
        .plotDoseData(plotDoses)
        .plotPredictionData(plotResponses)
        .weights(weights)
        .curveOutputParameters(curvePropertieDTOs)
        .build();
    kafkaProducerService.sendCurveData(curveDTO);
  }

  private DRCInputDTO collectCurveFitInputData(PlateDTO plate, List<WellDTO> plateWells,
      ResultDataDTO resultData, FeatureDTO feature, String substanceName) {
    var wells = plateWells.stream()
        .filter(w -> w.getWellSubstance() != null && substanceName.equals(
            w.getWellSubstance().getName()))
        .toList();

    var validWells = wells.stream()
        .filter((w -> w.getWellSubstance().getConcentration() > 0.0))
        .filter(w -> w.getStatus().getCode() >= 0)
        .filter(w -> w.getWellType().equalsIgnoreCase("SAMPLE"))
        .toList();

    var drcModelDTO = feature.getDrcModel();

    long[] wellIds = new long[validWells.size()];
    float[] concs = new float[validWells.size()];
    float[] accepts = new float[validWells.size()];
    float[] values = new float[validWells.size()];

    range(0, validWells.size()).forEach(i -> {
      // Set the well id
      wellIds[i] = validWells.get(i).getId();

      // Set the well substance concentration value
      float conc = validWells.get(i).getWellSubstance().getConcentration().floatValue();
      concs[i] = conc;

      // Set the well accept value (true or false)
      accepts[i] = (validWells.get(i).getStatus().getCode() >= 0
          && plate.getValidationStatus().getCode() >= 0 && plate.getApprovalStatus().getCode() >= 0)
          ? 1 : 0;

      // Set the well feature value
      var valueIndex =
          WellNumberUtils.getWellNr(validWells.get(i).getRow(), validWells.get(i).getColumn(),
              plate.getColumns()) - 1;
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

  private Map<String, Object> collectCurveFitOutputData(ScriptExecutionOutputDTO outputDTO) {
    try {
      OutputWrapper outputWrapper = objectMapper.readValue(outputDTO.getOutput(), OutputWrapper.class);
      return outputWrapper.output;
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  private ScriptExecutionRequest executeCurveFittingModel(DRCInputDTO inputDTO) {
    logger.info(
        String.format("Fitting curve for substance %s and feature ID %s", inputDTO.getSubstance(),
            inputDTO.getFeature().getId()));

    var inputVariables = new HashMap<String, Object>();
    inputVariables.put("substance", inputDTO.getSubstance());
    inputVariables.put("doses", inputDTO.getConcs());
    inputVariables.put("responses", inputDTO.getValues());
    inputVariables.put("accepts", inputDTO.getAccepts());

    if (inputDTO.getDrcModel().isPresent()) {
      try {
        DRCModelDTO drcModel = inputDTO.getDrcModel().get();
        logger.info("Input DRCModel: " + drcModel);

        var drcModelId = drcModel.getInputParameters().stream()
            .filter(inParam -> inParam.name().equals("drcModelId"))
            .findFirst()
            .orElseThrow(
                () -> new NoDRCModelDefinedForFeature("No DRCModel provided for feature %s (%d)",
                    inputDTO.getFeature().getName(), inputDTO.getFeature().getId()))
            .value();

        var curveFittingFormula = formulaService.getFormulaById(Long.parseLong(drcModelId));
        var script = curveFittingFormula.getFormula();

        inputVariables.put("fixedBottom", drcModel.getInputParameters().stream()
            .filter(inParam -> inParam.name().equals("fixedBottom"))
            .findFirst().orElseGet(() -> new InputParameter("fixedBottom", "NA"))
            .value());
        inputVariables.put("fixedTop", drcModel.getInputParameters().stream()
            .filter(inParam -> inParam.name().equals("fixedTop"))
            .findFirst().orElseGet(() -> new InputParameter("fixedTop", "NA"))
            .value());
        inputVariables.put("fixedSlope", drcModel.getInputParameters().stream()
            .filter(inParam -> inParam.name().equals("fixedSlope"))
            .findFirst().orElseGet(() -> new InputParameter("fixedSlope", "NA"))
            .value());
        inputVariables.put("confLevel", drcModel.getInputParameters().stream()
            .filter(inParam -> inParam.name().equals("confLevel"))
            .findFirst().orElseGet(() -> new InputParameter("confLevel", "0.95"))
            .value());
        inputVariables.put("robustMethod", drcModel.getInputParameters().stream()
            .filter(inParam -> inParam.name().equals("robustMethod"))
            .findFirst().orElseGet(() -> new InputParameter("robustMethod", "mean"))
            .value());
        inputVariables.put("slopeType", drcModel.getInputParameters().stream()
            .filter(inParam -> inParam.name().equals("slopeType"))
            .findFirst().orElseGet(() -> new InputParameter("slopeType", "ascending"))
            .value());
        inputVariables.put("responseName", inputDTO.getFeature().getName());

        return scriptExecutionService.submit(ScriptLanguage.R, script,
            FormulaCategory.CURVE_FITTING.name(), inputVariables);
      } catch (FormulaNotFoundException e) {
        throw new NoDRCModelDefinedForFeature("No DRCModel defined for feature %s (%d)",
            inputDTO.getFeature().getName(), inputDTO.getFeature().getId());
      }
    } else {
      throw new NoDRCModelDefinedForFeature("No DRCModel defined for feature %s (%d)",
          inputDTO.getFeature().getName(), inputDTO.getFeature().getId());
    }
  }

  private static class OutputWrapper {

    public final Map<String, Object> output;

    @JsonCreator
    private OutputWrapper(@JsonProperty(value = "output", required = true) Map<String, Object> output) {
      this.output = output;
    }
  }

  private static class DRCOutputDTO {

    public String pIC50toReport;
    public RangeResultsDTO rangeResults;
    public DataPredict2PlotDTO[] dataPredict2Plot;
    public float[] weights;
    public ModelCoefsDTO modelCoefs;
    public String residualVariance;
    public String warning;

    public DRCOutputDTO(@JsonProperty(value = "pIC50toReport") String pIC50toReport,
        @JsonProperty(value = "rangeResults") RangeResultsDTO rangeResults,
        @JsonProperty(value = "dataPredict2Plot") DataPredict2PlotDTO[] dataPredict2Plot,
        @JsonProperty(value = "weights") float[] weights,
        @JsonProperty(value = "modelCoefs") ModelCoefsDTO modelCoefs,
        @JsonProperty(value = "residulaVariance") String residualVariance,
        @JsonProperty(value = "warningFit") String warning) {
      this.pIC50toReport = pIC50toReport;
      this.rangeResults = rangeResults;
      this.dataPredict2Plot = dataPredict2Plot;
      this.weights = weights;
      this.modelCoefs = modelCoefs;
      this.residualVariance = residualVariance;
      this.warning = warning;
    }
  }

  private static class DataPredict2PlotDTO {

    public float dose;
    public float prediction;
    public float lower;
    public float upper;

    @JsonCreator
    private DataPredict2PlotDTO(@JsonProperty(value = "dose") float dose,
        @JsonProperty(value = "Prediction") float prediction,
        @JsonProperty(value = "Lower") float lower,
        @JsonProperty(value = "Upper") float upper) {
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
    private ModelCoefsDTO(@JsonProperty(value = "Slope") String[] slope,
        @JsonProperty(value = "Bottom") String[] bottom,
        @JsonProperty(value = "Top") String[] top,
        @JsonProperty(value = "negLog10ED50") String[] negLog10ED50) {
      this.slope = new ModelCoefDTO(slope);
      this.bottom = new ModelCoefDTO(bottom);
      this.top = new ModelCoefDTO(top);
      this.negLog10ED50 = new ModelCoefDTO(negLog10ED50);
    }
  }

  private static class ModelCoefDTO {

    public float estimate;
    public float stdError;
    public float tValue;
    public float pValue;
    public float lowerCI;
    public float upperCI;

    @JsonCreator
    public ModelCoefDTO(String[] values) {
      this.estimate = parseFloat(values[0]);
      this.stdError = parseFloat(values[1]);
      this.tValue = parseFloat(values[2]);
      this.pValue = parseFloat(values[3]);
      this.lowerCI = parseFloat(values[4]);
      this.upperCI = parseFloat(values[5]);
    }
  }

  private static class RangeResultsDTO {

    public RangeResultDTO eMax;
    public RangeResultDTO eMin;

    @JsonCreator
    public RangeResultsDTO(@JsonProperty(value = "eMax") RangeResultDTO[] eMax,
        @JsonProperty(value = "eMin") RangeResultDTO[] eMin) {
      this.eMax = (eMax != null && eMax.length > 0) ? eMax[0] : null;
      this.eMin = (eMin != null && eMin.length > 0) ? eMin[0] : null;
    }
  }

  private static class RangeResultDTO {

    public float dose;
    public float response;

    @JsonCreator
    public RangeResultDTO(@JsonProperty(value = "dose") String dose,
        @JsonProperty(value = "response") String response) {
      this.dose = parseFloat(dose);
      this.response = parseFloat(response);
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

  private static float parseFloat(String jsonValue) {
    return isCreatable(jsonValue) ? Float.parseFloat(jsonValue) : Float.NaN;
  }
}
