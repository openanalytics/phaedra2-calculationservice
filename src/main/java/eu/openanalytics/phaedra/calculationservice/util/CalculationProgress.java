package eu.openanalytics.phaedra.calculationservice.util;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import eu.openanalytics.phaedra.calculationservice.model.CalculationContext;
import eu.openanalytics.phaedra.plateservice.dto.WellDTO;
import eu.openanalytics.phaedra.protocolservice.dto.FeatureDTO;
import eu.openanalytics.phaedra.protocolservice.dto.FeatureStatDTO;
import eu.openanalytics.phaedra.resultdataservice.dto.ResultDataDTO;
import eu.openanalytics.phaedra.resultdataservice.dto.ResultFeatureStatDTO;

public class CalculationProgress {

	private CalculationContext ctx;
	private Integer currentSequence;
	
	private Map<Long, Boolean> featureDataUploaded;
	private Map<Long, Map<String, Boolean>> featureStatsUploaded;

	public CalculationProgress(CalculationContext ctx) {
		this.ctx = ctx;
		this.featureDataUploaded = new HashMap<>();
		this.featureStatsUploaded = new HashMap<>();
		
		incrementCurrentSequence();
		
		for (FeatureDTO f: ctx.getProtocolData().protocol.getFeatures()) {
			updateProgressFeature(f.getId(), false);
		}
	}
	
	public void updateProgressFeature(long fId, boolean status) {
		updateProgress(fId, null, null, status);
		
		List<FeatureStatDTO> stats = ctx.getProtocolData().featureStats.get(fId);
		List<String> wellTypes = ctx.getWells().stream().map(WellDTO::getWellType).toList();
		for (FeatureStatDTO stat: stats) {
			if (stat.getPlateStat()) {
				updateProgress(fId, stat.getId(), null, status);
			} else {
				for (String wt: wellTypes) {
					updateProgress(fId, stat.getId(), wt, status);
				}
			}
		}
	}
	
	public void updateProgress(Object rsObject) {
		if (rsObject instanceof ResultDataDTO) {
			ResultDataDTO rs = (ResultDataDTO) rsObject;
			updateProgress(rs.getFeatureId(), null, null, true);
		} else if (rsObject instanceof ResultFeatureStatDTO) {
			ResultFeatureStatDTO fs = (ResultFeatureStatDTO) rsObject;
			updateProgress(fs.getFeatureId(), fs.getFeatureStatId(), fs.getWelltype(), true);	
		}
	}
	
	private synchronized void updateProgress(long fId, Long statId, String wellType, boolean status) {
		if (statId == null) {
			featureDataUploaded.put(fId, status);
		} else if (wellType == null) {
			if (featureStatsUploaded.get(fId) == null) featureStatsUploaded.put(fId, new HashMap<>());
			featureStatsUploaded.get(fId).put(String.valueOf(statId), status);
		} else {
			if (featureStatsUploaded.get(fId) == null) featureStatsUploaded.put(fId, new HashMap<>());
			featureStatsUploaded.get(fId).put(String.format("%d_%s", statId, wellType), status);
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
		return ctx.getProtocolData().protocol.getFeatures().stream()
			.filter(f -> currentSequence.equals(f.getSequence()))
			.allMatch(f -> isFeatureComplete(f.getId()));
	}
	
	public synchronized void incrementCurrentSequence() {
		List<Integer> sequences = ctx.getProtocolData().sequences.keySet().stream().sorted().toList();
		if (sequences.isEmpty()) return;
		if (currentSequence == null) {
			currentSequence = sequences.get(0);
		} else {
			int sIndex = sequences.indexOf(currentSequence);
			if (sIndex + 1 < sequences.size()) currentSequence = sequences.get(sIndex + 1);
		}
	}
	
	public synchronized float getCompletedFraction() {
		long calcCount = featureDataUploaded.size() + featureStatsUploaded.values().stream().flatMap(m -> m.values().stream()).count();
		long completeCount = 
				featureDataUploaded.values().stream().filter(v -> v).count()
				+
				featureStatsUploaded.values().stream().flatMap(m -> m.values().stream()).filter(v -> v).count();
		return (float) completeCount / calcCount;
	}
	
	public synchronized boolean isComplete() {
		return getCompletedFraction() == 1.0f;
	}
}
