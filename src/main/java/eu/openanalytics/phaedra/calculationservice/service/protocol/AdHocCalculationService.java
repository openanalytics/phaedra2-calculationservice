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

import eu.openanalytics.phaedra.calculationservice.dto.ScriptExecutionOutputDTO;
import eu.openanalytics.phaedra.calculationservice.enumeration.ResponseStatusCode;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.regex.Pattern;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import eu.openanalytics.phaedra.calculationservice.dto.AdHocCalculationRequestDTO;
import eu.openanalytics.phaedra.calculationservice.dto.AdHocCalculationResponseDTO;
import eu.openanalytics.phaedra.calculationservice.dto.AdHocCalculationResponseDTO.AdHocResultData;
import eu.openanalytics.phaedra.calculationservice.exception.CalculationException;
import eu.openanalytics.phaedra.calculationservice.execution.CalculationContext;
import eu.openanalytics.phaedra.calculationservice.execution.input.InputGroup;
import eu.openanalytics.phaedra.calculationservice.execution.input.strategy.InputGroupingStrategy;
import eu.openanalytics.phaedra.calculationservice.execution.input.strategy.StrategyProvider;
import eu.openanalytics.phaedra.calculationservice.execution.script.ScriptExecutionRequest;
import eu.openanalytics.phaedra.calculationservice.execution.script.ScriptExecutionService;
import eu.openanalytics.phaedra.calculationservice.model.Formula;
import eu.openanalytics.phaedra.calculationservice.service.FormulaService;
import eu.openanalytics.phaedra.calculationservice.service.protocol.ProtocolDataCollector.ProtocolData;
import eu.openanalytics.phaedra.measservice.dto.MeasurementDTO;
import eu.openanalytics.phaedra.measurementservice.client.MeasurementServiceClient;
import eu.openanalytics.phaedra.plateservice.client.PlateServiceClient;
import eu.openanalytics.phaedra.plateservice.client.exception.UnresolvableObjectException;
import eu.openanalytics.phaedra.plateservice.dto.PlateDTO;
import eu.openanalytics.phaedra.plateservice.dto.PlateMeasurementDTO;
import eu.openanalytics.phaedra.plateservice.dto.WellDTO;
import eu.openanalytics.phaedra.protocolservice.dto.CalculationInputValueDTO;
import eu.openanalytics.phaedra.protocolservice.dto.FeatureDTO;
import eu.openanalytics.phaedra.protocolservice.enumeration.FeatureType;
import eu.openanalytics.phaedra.protocolservice.enumeration.InputSource;
import eu.openanalytics.phaedra.resultdataservice.dto.ResultDataDTO;

@Service
public class AdHocCalculationService {

	private final FormulaService formulaService;
	private final ScriptExecutionService scriptExecutionService;
	private final StrategyProvider strategyProvider;
	private final PlateServiceClient plateServiceClient;
	private final MeasurementServiceClient measurementServiceClient;

	private final Logger logger = LoggerFactory.getLogger(getClass());

	private final static String WILDCARD_SYMBOL = "*";
	private final static String WILDCARD_REGEX = ".*";

	public AdHocCalculationService(FormulaService formulaService, ScriptExecutionService scriptExecutionService,
			StrategyProvider strategyProvider, PlateServiceClient plateServiceClient, MeasurementServiceClient measurementServiceClient) {

		this.formulaService = formulaService;
		this.scriptExecutionService = scriptExecutionService;
		this.strategyProvider = strategyProvider;
		this.plateServiceClient = plateServiceClient;
		this.measurementServiceClient = measurementServiceClient;
	}

	public AdHocCalculationResponseDTO execute(AdHocCalculationRequestDTO calcRequest) throws CalculationException {
		AdHocCalculationResponseDTO response = new AdHocCalculationResponseDTO();
		response.setResultData(new ArrayList<>());

		ProtocolData protocolData = new ProtocolData();
		protocolData.formulas = formulaService.getFormulasByIds(Collections.singletonList(calcRequest.getFormulaId()));
		Formula formula = protocolData.formulas.get(calcRequest.getFormulaId());
		if (formula == null) throw new CalculationException("Invalid formula ID: %d", calcRequest.getFormulaId());

		for (Long plateId: calcRequest.getPlateIds()) {
			long startTime = System.currentTimeMillis();

			PlateDTO plate;
			List<WellDTO> wells;
			try {
				plate = plateServiceClient.getPlate(plateId);
				wells = plateServiceClient.getWells(plateId);
			} catch (UnresolvableObjectException e) {
				throw new CalculationException("Invalid plate ID: %d", plateId);
			}
			Long measId = (calcRequest.getMeasIds() == null) ? null : calcRequest.getMeasIds().get(plateId);
			if (measId == null) {
				// If no meas ID is given in the request, try to look up the plate's active measurement.
				try {
					List<PlateMeasurementDTO> measurements = plateServiceClient.getPlateMeasurements(plateId);
					measId = measurements.stream().filter(m -> m.getActive()).findAny().map(m -> m.getMeasurementId()).orElse(null);
				} catch (UnresolvableObjectException e) {
					throw new CalculationException("No measurement found for plate ID: %d", plateId);
				}
			}

			// Note: for ad-hoc calculations, there is no resultSetId. Its result data is not persistent.
			CalculationContext ctx = CalculationContext.create(protocolData, plate, wells, null, measId);

			long plateLoadDuration = System.currentTimeMillis() - startTime;
			long dataLoadDuration = 0;
			long scriptExecDuration = 0;
			startTime = System.currentTimeMillis();

			int scriptCount = 0;
			Semaphore threadBlocker = new Semaphore(0);

			List<FeatureDTO> features = makeAdHocFeatures(calcRequest, measId);
			Map<Pair<FeatureDTO,String>, ScriptExecutionOutputDTO> outputs = Collections.synchronizedMap(new HashMap<>());

			for (FeatureDTO feature: features) {
				long dataLoadStartTime = System.currentTimeMillis();

				InputGroupingStrategy groupingStrategy = strategyProvider.getStrategy(ctx, feature);
				Set<InputGroup> groups = groupingStrategy.createGroups(ctx, feature);

				long dataLoadEndTime = System.currentTimeMillis();
				dataLoadDuration += (dataLoadEndTime - dataLoadStartTime);

				for (InputGroup group: groups) {
					String groupKey = String.valueOf(group.getGroupNumber());
					ScriptExecutionRequest request = scriptExecutionService.submit(formula.getLanguage(), formula.getFormula(), formula.getCategory().name(), group.getInputVariables());
					scriptCount++;
					request.addCallback(output -> {
						outputs.put(Pair.of(feature, groupKey), output);
						threadBlocker.release(1);
					});
				}
			}

			// Block until all script executions are done.
			try {
				threadBlocker.acquire(scriptCount);
			} catch (InterruptedException e) {}

			scriptExecDuration = (System.currentTimeMillis() - startTime) - dataLoadDuration;

			// Assemble all outputs and store in the response object
			for (FeatureDTO feature: features) {
				Map<String, ScriptExecutionOutputDTO> featureOutputs = new HashMap<>();

				for (Pair<FeatureDTO,String> outputKey: outputs.keySet()) {
					if (outputKey.getLeft() != feature) continue;
					ScriptExecutionOutputDTO output = outputs.get(outputKey);
					if (output.getStatusCode() != ResponseStatusCode.SUCCESS) {
						throw new CalculationException("Script error on formula %d: %s", formula.getId(), output.getStatusMessage());
					}
					featureOutputs.put(outputKey.getRight(), output);
				}

				ResultDataDTO resultData = strategyProvider.getStrategy(ctx, feature).mergeOutput(ctx, feature, featureOutputs);
				response.getResultData().add(new AdHocResultData(plateId, measId, feature.getName(), resultData.getValues()));
			}

			logger.debug(String.format("AdHoc Calculation [Plate %d] [Meas %d] [%d Features]: plateLoad: %d ms, inputDataLoad: %d ms, scriptExec: %d ms.",
					plateId, measId, features.size(), plateLoadDuration, dataLoadDuration, scriptExecDuration));
		}

		return response;
	}

	/**
	 * Will instantiate one or more Features, depending on the presence of wildcards in the civs.
	 * If no wildcards are present, a single Feature is created.
	 * If wildcards are present, a number of Features is created equal to the number of meas columns
	 * that match the wildcarded pattern.
	 */
	private List<FeatureDTO> makeAdHocFeatures(AdHocCalculationRequestDTO calcRequest, long measId) {
		List<FeatureDTO> features = new ArrayList<>();

		// Support wildcards in the CIV names: look up and replace with actual column names
		Map<String, List<String>> civNames = new HashMap<String, List<String>>();
		for (Entry<String,String> entry: calcRequest.getCivs().entrySet()) {
			if (entry.getValue().contains(WILDCARD_SYMBOL)) {
				MeasurementDTO meas = measurementServiceClient.getMeasurementByMeasId(measId);
				if (meas == null) throw new CalculationException("Invalid measurement ID: %s", measId);

				Pattern namePattern = Pattern.compile(entry.getValue().replace(WILDCARD_SYMBOL, WILDCARD_REGEX));
				List<String> names = Arrays.stream(meas.getWellColumns()).filter(c -> namePattern.matcher(c).matches()).toList();
				civNames.put(entry.getKey(), names);
			} else {
				civNames.put(entry.getKey(), Collections.singletonList(entry.getValue()));
			}
		}

		long featureCount = civNames.values().stream().mapToLong(list -> list.size()).max().orElse(0);
		for (int i = 0; i < featureCount; i++) {
			FeatureDTO feature = new FeatureDTO();
			feature.setType(FeatureType.CALCULATION);
			feature.setSequence(0);
			feature.setFormulaId(calcRequest.getFormulaId());

			List<CalculationInputValueDTO> civs = new ArrayList<>();
			for (Entry<String,List<String>> entry: civNames.entrySet()) {
				int index = (entry.getValue().size() > i) ? i : 0;
				String colName = entry.getValue().get(index);

				civs.add(CalculationInputValueDTO.builder()
						.variableName(entry.getKey())
						.sourceMeasColName(colName)
						//TODO Support other types of InputSource
						.inputSource(InputSource.MEASUREMENT_WELL_COLUMN)
						.build()
				);

				if (entry.getValue().size() > 1) {
					feature.setName(colName);
				}
			}

			feature.setCivs(civs);
			features.add(feature);
		}

		return features;
	}
}
