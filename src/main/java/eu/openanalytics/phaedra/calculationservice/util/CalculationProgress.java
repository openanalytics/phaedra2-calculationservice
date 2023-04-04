package eu.openanalytics.phaedra.calculationservice.util;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import eu.openanalytics.phaedra.calculationservice.service.protocol.ProtocolDataCollector.ProtocolData;
import eu.openanalytics.phaedra.protocolservice.dto.FeatureStatDTO;
import eu.openanalytics.phaedra.resultdataservice.dto.ResultDataDTO;
import eu.openanalytics.phaedra.resultdataservice.dto.ResultFeatureStatDTO;

public class CalculationProgress {

	private ProtocolData protocolData;
	private Integer currentSequence;
	
	private Map<Long, Boolean> featureDataUploaded;
	private Map<Long, Map<Long, Boolean>> featureStatsUploaded;

	public CalculationProgress(ProtocolData protocolData) {
		this.protocolData = protocolData;
		this.featureDataUploaded = new HashMap<>();
		this.featureStatsUploaded = new HashMap<>();
		
		incrementCurrentSequence();
		
		protocolData.protocol.getFeatures().forEach(f -> {
			featureDataUploaded.put(f.getId(), false);
			Map<Long, Boolean> statsUploaded = new HashMap<>();
			List<FeatureStatDTO> stats = protocolData.featureStats.get(f.getId());
			if (stats != null) {
				stats.forEach(fs -> statsUploaded.put(fs.getId(), false));
			}
			featureStatsUploaded.put(f.getId(), statsUploaded);
		});
	}
	
	public synchronized void updateProgress(Object rsObject) {
		if (rsObject instanceof ResultDataDTO) {
			long fId = ((ResultDataDTO) rsObject).getFeatureId();
			featureDataUploaded.put(fId, true);
		} else if (rsObject instanceof ResultFeatureStatDTO) {
			ResultFeatureStatDTO fs = (ResultFeatureStatDTO) rsObject;
			long fId = fs.getFeatureId();
			Map<Long, Boolean> statsProgress = featureStatsUploaded.get(fId);
			if (statsProgress != null) statsProgress.put(fs.getId(), true);
		}
	}
	
	public synchronized boolean isFeatureComplete(long featureId) {
		return featureDataUploaded.get(featureId) && featureStatsUploaded.get(featureId).values().stream().allMatch(v -> v);
	}
	
	public Integer getCurrentSequence() {
		return currentSequence;
	}
	
	public synchronized boolean isCurrentSequenceComplete() {
		if (currentSequence == null) return false;
		return protocolData.protocol.getFeatures().stream()
			.filter(f -> currentSequence.equals(f.getSequence()))
			.allMatch(f -> isFeatureComplete(f.getId()));
	}
	
	public synchronized void incrementCurrentSequence() {
		List<Integer> sequences = protocolData.sequences.keySet().stream().sorted().toList();
		if (sequences.isEmpty()) return;
		if (currentSequence == null) {
			currentSequence = sequences.get(0);
		} else {
			int sIndex = sequences.indexOf(currentSequence);
			if (sIndex + 1 < sequences.size()) currentSequence = sequences.get(sIndex + 1);
		}
	}
	
	public synchronized boolean isComplete() {
		return (
				featureDataUploaded.values().stream().allMatch(v -> v)
				&&
				featureStatsUploaded.values().stream().flatMap(m -> m.values().stream()).allMatch(v -> v));
	}
}
