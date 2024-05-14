package eu.openanalytics.phaedra.calculationservice.execution.progress;

import static eu.openanalytics.phaedra.calculationservice.util.LoggerHelper.log;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.openanalytics.phaedra.calculationservice.execution.CalculationContext;
import eu.openanalytics.phaedra.calculationservice.execution.script.ScriptExecutionRequest;
import eu.openanalytics.phaedra.protocolservice.dto.FeatureDTO;
import eu.openanalytics.phaedra.resultdataservice.dto.ResultDataDTO;
import eu.openanalytics.phaedra.resultdataservice.dto.ResultFeatureStatDTO;
import eu.openanalytics.phaedra.resultdataservice.enumeration.StatusCode;
import eu.openanalytics.phaedra.scriptengine.dto.ResponseStatusCode;
import lombok.Builder;

/**
 * This class tracks the state (progress) of an ongoing calculation execution.
 * Listeners can be attached to respond to various calculation lifecycle events.
 * 
 * The calculation process is split into CalculationStages.
 * A typical calculation lifecycle looks like this:
 * 
 * <ol>
 * <li>Protocol Started</li>
 * <li>Sequence1 Started</li>
 * <li>Feature1 Formula Started</li>
 * <li>Feature1 Formula ScriptOutputAvailable</li>
 * <li>Feature1 Formula Complete</li>
 * <li>Feature1 Statistics Started</li>
 * <li>Feature1 Statistics ScriptOutputAvailable</li>
 * <li>Feature1 Statistics Complete</li>
 * <li>Feature1 CurveFit Started</li>
 * <li>Feature1 CurveFit ScriptOutputAvailable</li>
 * <li>Feature1 CurveFit Complete</li>
 * <li>Feature2 Formula Started</li>
 * <li>...</li>
 * <li>Sequence1 Complete</li>
 * <li>Sequence2 Started</li>
 * <li>...</li>
 * <li>Protocol Complete</li>
 * </ol>
 */
public class CalculationStateTracker {

	private CalculationContext ctx;
	private Integer currentSequence;
	private SequenceProgress sequenceProgress;
	private Set<TrackedScriptExecution> trackedExecutions;
	private List<BiConsumer<CalculationStateEvent,Set<ScriptExecutionRequest>>> listeners;
	
	private final Logger logger = LoggerFactory.getLogger(getClass());
	
	public CalculationStateTracker(CalculationContext ctx) {
		this.ctx = ctx;
		sequenceProgress = new SequenceProgress();
		trackedExecutions = Collections.synchronizedSet(new HashSet<>());
		listeners = Collections.synchronizedList(new ArrayList<>());
	}
	
	public void startStage(long featureId, CalculationStage stage, int size) {
		sequenceProgress.initStage(featureId, stage, size);
	}
	
	public void trackScriptExecution(long featureId, CalculationStage stage, ScriptExecutionRequest request, Map<String, Object> context) {
		TrackedScriptExecution trackedExec = TrackedScriptExecution.builder()
				.featureId(featureId)
				.stage(stage)
				.request(request)
				.context(context)
				.build();
		trackedExecutions.add(trackedExec);
		request.addCallback(output -> {
			trackedExec.request.setOutput(output);
			CalculationStateEventCode stageOutcome = (output.getStatusCode() == ResponseStatusCode.SUCCESS) ? CalculationStateEventCode.ScriptOutputAvailable : CalculationStateEventCode.Error;
			updateProgress(trackedExec.featureId, trackedExec.stage, stageOutcome);
		});
	}
	
	public void handleResultSetUpdate(Object payload) {
		if (payload instanceof ResultDataDTO) {
			// ResultData for a feature has been saved successfully or encountered an error
			long featureId = ((ResultDataDTO) payload).getFeatureId();
			StatusCode statusCode = ((ResultDataDTO) payload).getStatusCode();
			if (statusCode == StatusCode.SUCCESS) {
				updateProgress(featureId, CalculationStage.FeatureFormula, CalculationStateEventCode.Complete);
			} else if (statusCode == StatusCode.FAILURE) {
				updateProgress(featureId, CalculationStage.FeatureFormula, CalculationStateEventCode.Error);
			}
		} else if (payload instanceof ResultFeatureStatDTO) {
			// A statistic for a feature has been saved successfully or encountered an error
			long featureId = ((ResultFeatureStatDTO) payload).getFeatureId();
			StatusCode statusCode = ((ResultFeatureStatDTO) payload).getStatusCode();
			if (statusCode == StatusCode.SUCCESS) {
				updateProgress(featureId, CalculationStage.FeatureStatistics, CalculationStateEventCode.Complete);
			} else if (statusCode == StatusCode.FAILURE) {
				updateProgress(featureId, CalculationStage.FeatureStatistics, CalculationStateEventCode.Error);
			}
		}
	}
	
	private void updateProgress(long featureId, CalculationStage stage, CalculationStateEventCode code) {
		if (sequenceProgress.hasErrors()) {
			// If the current sequence already encountered an error, do not process any further codes.
			return;
		}
		
		sequenceProgress.addOne(featureId, stage, code);
		
		CalculationStateEventCode stageOutcome = sequenceProgress.getStageOutcome(featureId, stage);
		if (stageOutcome != null) {
			emit(new CalculationStateEvent(stage, stageOutcome, featureId));
		}
		
		CalculationStateEventCode sequenceOutcome = sequenceProgress.getSequenceOutcome(currentSequence, ctx);
		if (sequenceOutcome != null) {
			emit(new CalculationStateEvent(CalculationStage.Sequence, sequenceOutcome, null));
		}
	}
	
	public void skipStage(long featureId, CalculationStage stage) {
		sequenceProgress.addAll(featureId, stage, CalculationStateEventCode.Complete);
	}
	
	public Integer getCurrentSequence() {
		return currentSequence;
	}

	public void incrementCurrentSequence() {
		List<Integer> sequences = ctx.getProtocolData().sequences.keySet().stream().sorted().toList();
		if (sequences.isEmpty()) {
			log(logger, ctx, "Nothing to calculate: no sequences defined");
			return;
		}
		
		if (currentSequence == null) {
			log(logger, ctx, "Executing protocol %d", ctx.getProtocolData().protocol.getId());
			emit(new CalculationStateEvent(CalculationStage.Protocol, CalculationStateEventCode.Started, null));
			startTrackingSequence(sequences.get(0));
		} else {
			int sIndex = sequences.indexOf(currentSequence);
			if (sIndex + 1 < sequences.size()) {
				startTrackingSequence(sequences.get(sIndex + 1));
			} else {
				// Last sequence ended
				emit(new CalculationStateEvent(CalculationStage.Protocol, CalculationStateEventCode.Complete, null));
			}
		}
	}
	
	private void startTrackingSequence(int sequence) {
		currentSequence = sequence;
		log(logger, ctx, "Executing sequence %d", currentSequence);
		sequenceProgress.reset();
		trackedExecutions.clear();
		emit(new CalculationStateEvent(CalculationStage.Sequence, CalculationStateEventCode.Started, null));
	}
	
	public void addEventListener(CalculationStage stage, CalculationStateEventCode code, Long featureId, Consumer<Set<ScriptExecutionRequest>> handler) {
		listeners.add(new FilterEventListener(stage, code, featureId, handler));
	}
	
	private void emit(CalculationStateEvent event) {
		Set<ScriptExecutionRequest> requests;
		synchronized (trackedExecutions) {
			requests = trackedExecutions.stream()
					.filter(exec -> (event.getFeatureId() == null || exec.featureId == event.getFeatureId()) 
							&& (event.getStage() == null || exec.stage == event.getStage()))
					.map(exec -> exec.request)
					.collect(Collectors.toSet());
		}
		synchronized (listeners) {
			listeners.forEach(l -> l.accept(event, requests));
//        	ForkJoinPool.commonPool().submit(() -> listeners.forEach(l -> l.accept(event)));
        }
	}

	public Map<String, Object> getRequestContext(ScriptExecutionRequest request) {
		synchronized (trackedExecutions) {
			return trackedExecutions.stream().filter(exec -> exec.request == request).findAny().map(trackedExec -> trackedExec.context).orElse(null);
		}
	}
	
	private static CalculationStateEventCode aggregateCodes(Set<CalculationStateEventCode> codes, int expectedSize) {
		if (codes == null || codes.isEmpty() || codes.size() < expectedSize) return null;
		if (codes.stream().anyMatch(code -> code == null)) return null;
		if (codes.stream().anyMatch(code -> code == CalculationStateEventCode.Error)) return CalculationStateEventCode.Error;
		return codes.iterator().next();
	}
	
	@Builder
	private static class TrackedScriptExecution {
		private Long featureId;
		private CalculationStage stage;
		private ScriptExecutionRequest request;
		private Map<String, Object> context;
	}

	@Builder
	private static class FilterEventListener implements BiConsumer<CalculationStateEvent,Set<ScriptExecutionRequest>> {
		
		private CalculationStage stage;
		private CalculationStateEventCode code;
		private Long featureId;
		private Consumer<Set<ScriptExecutionRequest>> handler;
		
		@Override
		public void accept(CalculationStateEvent event, Set<ScriptExecutionRequest> requests) {
			if ((stage == null || event.getStage() == stage) && (code == null || event.getCode() == code) && (featureId == null || event.getFeatureId() == featureId)) {
				handler.accept(requests);
			}
		}
	}

	private static class SequenceProgress {
		
		private Map<Pair<Long, CalculationStage>, StageProgress> progressMap = new HashMap<>();
		
		public synchronized void reset() {
			progressMap.clear();
		}
		
		public synchronized void initStage(long featureId, CalculationStage stage, int size) {
			progressMap.put(Pair.of(featureId, stage), new StageProgress(size));
		}
		
		public synchronized void addOne(long featureId, CalculationStage stage, CalculationStateEventCode code) {
			StageProgress stageProgress = progressMap.get(Pair.of(featureId, stage));
			if (stageProgress == null) {
				throw new RuntimeException("Stage has not been initialized: " + stage + " for feature " + featureId);
			}
			stageProgress.add(code);
		}
		
		public synchronized void addAll(long featureId, CalculationStage stage, CalculationStateEventCode code) {
			StageProgress stageProgress = progressMap.get(Pair.of(featureId, stage));
			if (stageProgress == null) {
				throw new RuntimeException("Stage has not been initialized: " + stage + " for feature " + featureId);
			}
			while (stageProgress.codes.size() < stageProgress.stageSize) {
				stageProgress.add(code);
			}
		}
		
		public boolean hasErrors() {
			return progressMap.values().stream().anyMatch(stage -> stage.getStageOutcome() == CalculationStateEventCode.Error);
		}
		
		public synchronized CalculationStateEventCode getStageOutcome(long featureId, CalculationStage stage) {
			StageProgress stageProgress = progressMap.get(Pair.of(featureId, stage));
			return Optional.ofNullable(stageProgress).map(p -> p.getStageOutcome()).orElse(null);
		}
		
		public synchronized CalculationStateEventCode getFeatureOutcome(long featureId) {
			List<StageProgress> stages = progressMap.entrySet().stream()
					.filter(e -> e.getKey().getLeft() == featureId).map(e -> e.getValue()).toList();
			return aggregateCodes(stages.stream().map(stage -> stage.getStageOutcome()).collect(Collectors.toSet()), 3);
		}
		
		public synchronized CalculationStateEventCode getSequenceOutcome(int sequence, CalculationContext ctx) {
			List<FeatureDTO> features = ctx.getProtocolData().sequences.get(sequence);
			return aggregateCodes(features.stream().map(f -> getFeatureOutcome(f.getId())).collect(Collectors.toSet()), features.size());
		}
	}
	
	private static class StageProgress {
		
		private int stageSize;
		private Set<CalculationStateEventCode> codes;
		
		public StageProgress(int stageSize) {
			this.stageSize = stageSize;
			this.codes = new HashSet<>();
		}
		
		public void add(CalculationStateEventCode code) {
			if (codes.size() < stageSize) codes.add(code);
		}
		
		public CalculationStateEventCode getStageOutcome() {
			return aggregateCodes(codes, stageSize);
		}
	}
}
