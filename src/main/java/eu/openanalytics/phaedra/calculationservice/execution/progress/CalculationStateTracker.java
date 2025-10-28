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
package eu.openanalytics.phaedra.calculationservice.execution.progress;

import static eu.openanalytics.phaedra.calculationservice.util.LoggerHelper.log;

import eu.openanalytics.phaedra.calculationservice.enumeration.ResponseStatusCode;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
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

	private List<BiConsumer<CalculationStateEvent,Map<String, ScriptExecutionRequest>>> eventListeners;
	private Executor eventExecutor;

	private final Logger logger = LoggerFactory.getLogger(getClass());

	public CalculationStateTracker(CalculationContext ctx) {
		this.ctx = ctx;
		sequenceProgress = new SequenceProgress();
		trackedExecutions = Collections.synchronizedSet(new HashSet<>());
		eventListeners = Collections.synchronizedList(new ArrayList<>());
		eventExecutor = Executors.newSingleThreadExecutor();
	}

	public void startStage(long featureId, CalculationStage stage, int size) {
		sequenceProgress.initStage(featureId, stage, size);
	}

	public void skipStage(long featureId, CalculationStage stage) {
		startStage(featureId, stage, 1);
		updateProgress(featureId, stage, "1", CalculationStateEventCode.Complete);
	}

	public void failStage(long featureId, CalculationStage stage, String errorMessage, Object... errorContext) {
		ctx.getErrorCollector().addError(errorMessage, errorContext);
		startStage(featureId, stage, 1);
		updateProgress(featureId, stage, "1", CalculationStateEventCode.Error);
	}

	public void trackScriptExecution(long featureId, CalculationStage stage, Object groupKey, ScriptExecutionRequest request) {
		String groupId = String.valueOf(groupKey);
		TrackedScriptExecution trackedExec = TrackedScriptExecution.builder()
				.featureId(featureId)
				.groupId(groupId)
				.stage(stage)
				.request(request)
				.build();
		trackedExecutions.add(trackedExec);
		request.addCallback(output -> {
			trackedExec.request.setOutput(output);
			CalculationStateEventCode stageOutcome = (output.getStatusCode() == ResponseStatusCode.SUCCESS) ? CalculationStateEventCode.ScriptOutputAvailable : CalculationStateEventCode.Error;
			updateProgress(trackedExec.featureId, trackedExec.stage, groupId, stageOutcome);
		});
	}

	public void handleResultSetUpdate(Object payload) {
		if (payload instanceof ResultDataDTO) {
			// ResultData for a feature has been saved successfully or encountered an error
			ResultDataDTO resultData = (ResultDataDTO) payload;
			if (resultData.getStatusCode() == StatusCode.SUCCESS) {
				updateProgress(resultData.getFeatureId(), CalculationStage.FeatureFormula, null, CalculationStateEventCode.Complete);
			} else if (resultData.getStatusCode() == StatusCode.FAILURE) {
				failStage(resultData.getFeatureId(), CalculationStage.FeatureFormula, "Failed to persist feature result values");
			}
		} else if (payload instanceof ResultFeatureStatDTO) {
			// A statistic for a feature has been saved successfully or encountered an error
			ResultFeatureStatDTO featureStat = (ResultFeatureStatDTO) payload;
			String groupId = String.format("%d-%s", featureStat.getFeatureStatId(), featureStat.getWelltype());
			if (featureStat.getStatusCode() == StatusCode.SUCCESS) {
				updateProgress(featureStat.getFeatureId(), CalculationStage.FeatureStatistics, groupId, CalculationStateEventCode.Complete);
			} else if (featureStat.getStatusCode() == StatusCode.FAILURE) {
				updateProgress(featureStat.getFeatureId(), CalculationStage.FeatureStatistics, groupId, CalculationStateEventCode.Error);
			}
		}
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

	public void addEventListener(CalculationStage stage, CalculationStateEventCode code, Long featureId, Consumer<Map<String, ScriptExecutionRequest>> handler) {
		eventListeners.add(new FilterEventListener(stage, code, featureId, handler));
	}

	private void startTrackingSequence(int sequence) {
		currentSequence = sequence;
		log(logger, ctx, "Executing sequence %d", currentSequence);
		sequenceProgress.reset();
		trackedExecutions.clear();
		emit(new CalculationStateEvent(CalculationStage.Sequence, CalculationStateEventCode.Started, null));
	}

	private void updateProgress(long featureId, CalculationStage stage, String groupId, CalculationStateEventCode code) {
		if (sequenceProgress.hasErrors()) {
			// If the current sequence already encountered an error, do not process any further codes.
			return;
		}

		sequenceProgress.add(featureId, stage, groupId, code);

		CalculationStateEventCode stageOutcome = sequenceProgress.getStageOutcome(featureId, stage);
		CalculationStateEventCode sequenceOutcome = sequenceProgress.getSequenceOutcome(currentSequence, ctx);

		logger.debug(String.format("Progress update [Feature %d] [Stage %s] [Group %s] = %s. Progress: [Stage: %s] [Sequence: %s]", featureId, stage, groupId, code, stageOutcome, sequenceOutcome));

		if (stageOutcome != null) {
			emit(new CalculationStateEvent(stage, stageOutcome, featureId));
		}

		if (sequenceOutcome != null) {
			emit(new CalculationStateEvent(CalculationStage.Sequence, sequenceOutcome, null));
		}
	}

	private void emit(CalculationStateEvent event) {
		Map<String, ScriptExecutionRequest> requests;
		synchronized (trackedExecutions) {
			requests = trackedExecutions.stream()
					.filter(exec -> (event.getFeatureId() == null || event.getFeatureId().equals(exec.featureId)))
					.filter(exec -> (event.getStage() == null || exec.stage == event.getStage()))
					.filter(exec -> (event.getCode() != CalculationStateEventCode.Error || exec.request.getOutput() != null))
					.collect(Collectors.toMap(exec -> exec.groupId, exec -> exec.request));
		}

		final List<BiConsumer<CalculationStateEvent,Map<String, ScriptExecutionRequest>>> listCopy = new ArrayList<>();
		synchronized (eventListeners) {
			listCopy.addAll(eventListeners);
		}

		eventExecutor.execute(() -> {
			listCopy.forEach(l -> l.accept(event, requests));
		});
	}

	private static CalculationStateEventCode aggregateCodes(List<CalculationStateEventCode> codes, int expectedSize) {
		if (codes == null || codes.isEmpty()) return null;
		if (codes.stream().anyMatch(code -> code == CalculationStateEventCode.Error)) return CalculationStateEventCode.Error;
		if (codes.size() < expectedSize || codes.stream().anyMatch(code -> code == null)) return null;
		if (codes.stream().allMatch(code -> code == CalculationStateEventCode.ScriptOutputAvailable)) return CalculationStateEventCode.ScriptOutputAvailable;
		if (codes.stream().allMatch(code -> code == CalculationStateEventCode.Complete)) return CalculationStateEventCode.Complete;
		return null;
	}

	@Builder
	private static class TrackedScriptExecution {
		private Long featureId;
		private CalculationStage stage;
		private String groupId;
		private ScriptExecutionRequest request;
	}

	@Builder
	private static class FilterEventListener implements BiConsumer<CalculationStateEvent,Map<String, ScriptExecutionRequest>> {

		private CalculationStage stage;
		private CalculationStateEventCode code;
		private Long featureId;
		private Consumer<Map<String, ScriptExecutionRequest>> handler;

		@Override
		public void accept(CalculationStateEvent event, Map<String, ScriptExecutionRequest> requests) {
			if ((stage == null || event.getStage() == stage) && (code == null || event.getCode() == code) && (featureId == null || featureId.equals(event.getFeatureId()))) {
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

		public synchronized void add(long featureId, CalculationStage stage, String id, CalculationStateEventCode code) {
			StageProgress stageProgress = progressMap.get(Pair.of(featureId, stage));
			if (stageProgress == null) {
				throw new RuntimeException("Stage has not been initialized: " + stage + " for feature " + featureId);
			}
			stageProgress.add(id, code);
		}

		public synchronized boolean hasErrors() {
			return progressMap.values().stream().anyMatch(stage -> stage.getStageOutcome() == CalculationStateEventCode.Error);
		}

		public synchronized CalculationStateEventCode getStageOutcome(long featureId, CalculationStage stage) {
			StageProgress stageProgress = progressMap.get(Pair.of(featureId, stage));
			return Optional.ofNullable(stageProgress).map(p -> p.getStageOutcome()).orElse(null);
		}

		public synchronized CalculationStateEventCode getFeatureOutcome(long featureId) {
			List<StageProgress> stages = progressMap.entrySet().stream()
					.filter(e -> e.getKey().getLeft() == featureId).map(e -> e.getValue()).toList();
			return aggregateCodes(stages.stream().map(stage -> stage.getStageOutcome()).toList(), 3);
		}

		public synchronized CalculationStateEventCode getSequenceOutcome(int sequence, CalculationContext ctx) {
			List<FeatureDTO> features = ctx.getProtocolData().sequences.get(sequence);
			return aggregateCodes(features.stream().map(f -> getFeatureOutcome(f.getId())).toList(), features.size());
		}
	}

	private static class StageProgress {

		private int stageSize;
		private Map<String, CalculationStateEventCode> codes;

		public StageProgress(int stageSize) {
			this.stageSize = stageSize;
			this.codes = new HashMap<>();
		}

		public void add(String id, CalculationStateEventCode code) {
			if (id == null) {
				// If no id is given, the whole stage will be updated.
				for (Entry<String, CalculationStateEventCode> entry : codes.entrySet()) {
					entry.setValue(code);
				}
			} else {
				codes.put(id, code);
			}
		}

		public CalculationStateEventCode getStageOutcome() {
			return aggregateCodes(codes.values().stream().toList(), stageSize);
		}
	}
}
