package eu.openanalytics.phaedra.calculationservice.service.protocol;

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

import org.apache.commons.lang3.tuple.MutablePair;
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
import eu.openanalytics.phaedra.scriptengine.dto.ResponseStatusCode;
import eu.openanalytics.phaedra.scriptengine.dto.ScriptExecutionOutputDTO;

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
		
		//TODO Parallellize execution across Plates, Features and InputGroups
		
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
			
			List<FeatureDTO> features = makeAdHocFeatures(calcRequest, measId);
			for (FeatureDTO feature: features) {
				long dataLoadStartTime = System.currentTimeMillis();
				
				InputGroupingStrategy groupingStrategy = strategyProvider.getStrategy(ctx, feature);
				Set<InputGroup> groups = groupingStrategy.createGroups(ctx, feature);
				
				long dataLoadEndTime = System.currentTimeMillis();
				
				Map<String, ScriptExecutionOutputDTO> outputs = new HashMap<>();
				for (InputGroup group: groups) {
					ScriptExecutionOutputDTO output = executeScript(formula, group);
					outputs.put(String.valueOf(group.getGroupNumber()), output);
				}
				
				long scriptExecutionEndTime = System.currentTimeMillis();
				
				ResultDataDTO resultData = groupingStrategy.mergeOutput(ctx, feature, outputs);
				response.getResultData().add(new AdHocResultData(plateId, measId, feature.getName(), resultData.getValues()));
				
				dataLoadDuration += (dataLoadEndTime - dataLoadStartTime);
				scriptExecDuration += (scriptExecutionEndTime - dataLoadEndTime);
			}
			
			logger.debug(String.format("AdHoc Calculation [Plate %d] [Meas %d] [%d Features]: plateLoad: %d ms, dataLoad: %d ms, scriptExec: %d ms.", 
					plateId, measId, features.size(), plateLoadDuration, dataLoadDuration, scriptExecDuration));
		}
		
		return response;
	}
	
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
		
		long featureCount = civNames.values().stream().flatMap(names -> names.stream()).distinct().count();
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
	
	private ScriptExecutionOutputDTO executeScript(Formula formula, InputGroup group) {
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
			throw new CalculationException("Script error on formula %d: %s", formula.getId(), scriptRef.getValue().getStatusMessage());
		}
		
		return scriptRef.getValue();
	}
}
