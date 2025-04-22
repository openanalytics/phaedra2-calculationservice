package eu.openanalytics.phaedra.calculationservice.service.protocol;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Semaphore;

import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import eu.openanalytics.phaedra.calculationservice.dto.AdHocCalculationRequestDTO;
import eu.openanalytics.phaedra.calculationservice.dto.AdHocCalculationResponseDTO;
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
import eu.openanalytics.phaedra.plateservice.client.PlateServiceClient;
import eu.openanalytics.phaedra.plateservice.client.exception.UnresolvableObjectException;
import eu.openanalytics.phaedra.plateservice.dto.PlateDTO;
import eu.openanalytics.phaedra.plateservice.dto.WellDTO;
import eu.openanalytics.phaedra.protocolservice.dto.CalculationInputValueDTO;
import eu.openanalytics.phaedra.protocolservice.dto.FeatureDTO;
import eu.openanalytics.phaedra.protocolservice.enumeration.FeatureType;
import eu.openanalytics.phaedra.protocolservice.enumeration.InputSource;
import eu.openanalytics.phaedra.resultdataservice.dto.ResultDataDTO;
import eu.openanalytics.phaedra.scriptengine.dto.ResponseStatusCode;
import eu.openanalytics.phaedra.scriptengine.dto.ScriptExecutionOutputDTO;

@Service
public class AdHocCalculationService {

	private final FormulaService formulaService;
	private final ScriptExecutionService scriptExecutionService;
	private final StrategyProvider strategyProvider;
	private final PlateServiceClient plateServiceClient;
	
	private final Logger logger = LoggerFactory.getLogger(getClass());
	
	public AdHocCalculationService(FormulaService formulaService, ScriptExecutionService scriptExecutionService, StrategyProvider strategyProvider, PlateServiceClient plateServiceClient) {
		this.formulaService = formulaService;
		this.scriptExecutionService = scriptExecutionService;
		this.strategyProvider = strategyProvider;
		this.plateServiceClient = plateServiceClient;
	}
	
	public AdHocCalculationResponseDTO execute(AdHocCalculationRequestDTO calcRequest) throws CalculationException {
		Map<Long, float[]> valuesPerPlate = new HashMap<>();
		
		ProtocolData protocolData = new ProtocolData();
		protocolData.formulas = formulaService.getFormulasByIds(Collections.singletonList(calcRequest.getFormulaId()));
		Formula formula = protocolData.formulas.get(calcRequest.getFormulaId());
		if (formula == null) throw new CalculationException("Invalid formula ID: %d", calcRequest.getFormulaId());
		
		//TODO Support other types of InputSource
		List<CalculationInputValueDTO> civs = calcRequest.getCivs().entrySet().stream().map(e -> {
			return CalculationInputValueDTO.builder()
					.variableName(e.getKey())
					.sourceMeasColName(e.getValue())
					.inputSource(InputSource.MEASUREMENT_WELL_COLUMN)
					.build();
		}).toList();
		
		FeatureDTO feature = new FeatureDTO();
		feature.setType(FeatureType.CALCULATION);
		feature.setName("AdHoc Feature");
		feature.setSequence(0);
		feature.setFormulaId(calcRequest.getFormulaId());
		feature.setCivs(civs);
		
		//TODO Parallellize execution across Plates and InputGroups
		
		for (Long plateId: calcRequest.getPlateIds()) {
			long startTime = System.currentTimeMillis();
			
			PlateDTO plate;
			List<WellDTO> wells;
			try {
				plate = plateServiceClient.getPlate(plateId);
				wells = plateServiceClient.getWells(plateId);
			} catch (UnresolvableObjectException e) {
				throw new CalculationException("Invalid plate IDs: %s", calcRequest.getPlateIds());
			}
			Long measId = (calcRequest.getMeasIds() == null) ? null : calcRequest.getMeasIds().get(plateId);
			if (measId == null) measId = plate.getMeasurementId();
			
			// Note: for ad-hoc calculations, there is no resultSetId. Its result data is not persistent.
			CalculationContext ctx = CalculationContext.create(protocolData, plate, wells, null, measId);
			
			InputGroupingStrategy groupingStrategy = strategyProvider.getStrategy(ctx, feature);
			Set<InputGroup> groups = groupingStrategy.createGroups(ctx, feature);
			Map<String, ScriptExecutionOutputDTO> outputs = new HashMap<>();
			
			long dataLoadEndTime = System.currentTimeMillis();
			
			for (InputGroup group: groups) {
				ScriptExecutionRequest request = scriptExecutionService.submit(formula.getLanguage(), formula.getFormula(), formula.getCategory().name(), group.getInputVariables());
				Pair<ScriptExecutionRequest, ScriptExecutionOutputDTO> scriptRef = MutablePair.of(request, null);
			
				// Block the thread until the script callback is done and output is available.
				Semaphore threadBlocker = new Semaphore(1);
				try { threadBlocker.acquire(); } catch (InterruptedException e) {}
				request.addCallback(output -> {
					scriptRef.setValue(output);
					threadBlocker.release();
				});
				try { threadBlocker.acquire(); } catch (InterruptedException e) {}
				
				if (scriptRef.getValue().getStatusCode() != ResponseStatusCode.SUCCESS) {
					throw new CalculationException("Script error on formula %d: %s", formula, scriptRef.getValue().getStatusMessage());
				}
				outputs.put(String.valueOf(group.getGroupNumber()), scriptRef.getValue());
			}

			long scriptExecutionEndTime = System.currentTimeMillis();
			long dataLoadDuration = dataLoadEndTime - startTime;
			long scriptExecDuration = scriptExecutionEndTime - dataLoadEndTime;
			logger.debug(String.format("AdHoc Calculation for plate %d: data loading: %d ms, script execution: %d ms.", plateId, dataLoadDuration, scriptExecDuration));
			
			ResultDataDTO resultData = groupingStrategy.mergeOutput(ctx, feature, outputs);
			valuesPerPlate.put(plateId, resultData.getValues());
		}
		
		return new AdHocCalculationResponseDTO(valuesPerPlate);
	}
	
}
