package eu.openanalytics.phaedra.calculationservice.util;

import static eu.openanalytics.phaedra.calculationservice.util.LoggerHelper.log;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

	private final Logger logger = LoggerFactory.getLogger(getClass());
	
	public CalculationProgress(CalculationContext ctx) {
		this.ctx = ctx;
		this.featureDataUploaded = new HashMap<>();
		this.featureStatsUploaded = new HashMap<>();
		
		incrementCurrentSequence();
		
		for (FeatureDTO f: ctx.getProtocolData().protocol.getFeatures()) {
			featureDataUploaded.put(f.getId(), false);
			
			List<FeatureStatDTO> stats = ctx.getProtocolData().featureStats.get(f.getId());
			List<String> wellTypes = ctx.getWells().stream().map(WellDTO::getWellType).toList();
			
			Map<String, Boolean> statsUploaded = new HashMap<>();
			for (FeatureStatDTO stat: stats) {
				if (stat.getPlateStat()) {
					statsUploaded.put(String.valueOf(stat.getId()), false);
				} else {
					for (String wt: wellTypes) {
						statsUploaded.put(String.format("%d_%s", stat.getId(), wt), false);	
					}
				}
			}
			featureStatsUploaded.put(f.getId(), statsUploaded);
		}
	}
	
	public synchronized void updateProgress(Object rsObject) {
		if (rsObject instanceof ResultDataDTO) {
			long fId = ((ResultDataDTO) rsObject).getFeatureId();
			featureDataUploaded.put(fId, true);
		} else if (rsObject instanceof ResultFeatureStatDTO) {
			ResultFeatureStatDTO fs = (ResultFeatureStatDTO) rsObject;
			Map<String, Boolean> statsProgress = featureStatsUploaded.get(fs.getFeatureId());
			if (fs.getWelltype() == null) {
				statsProgress.put(String.valueOf(fs.getFeatureStatId()), true);				
			} else {
				statsProgress.put(String.format("%d_%s", fs.getFeatureStatId(), fs.getWelltype()), true);
			}
		}
		
		for (Long id: featureStatsUploaded.keySet()) {
			for (String key: featureStatsUploaded.get(id).keySet()) {
				logger.info(String.format("%d_%s = %s", id, key, String.valueOf(featureStatsUploaded.get(id).get(key))));
			}
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
