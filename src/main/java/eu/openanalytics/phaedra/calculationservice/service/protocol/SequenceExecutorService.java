/**
 * Phaedra II
 *
 * Copyright (C) 2016-2023 Open Analytics
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

import static eu.openanalytics.phaedra.calculationservice.util.LoggerHelper.log;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import eu.openanalytics.phaedra.calculationservice.model.CalculationContext;
import eu.openanalytics.phaedra.calculationservice.service.script.ScriptExecutionRequest;

/**
 * A sequence is a group of features that may depend on features from the
 * previous sequence, but do not depend on features from the same sequence
 * or any later sequence.
 * 
 * In other words, all features from a sequence can be calculated in parallel.
 */
@Service
public class SequenceExecutorService {

    private final FeatureExecutorService featureExecutorService;
    
    private final Logger logger = LoggerFactory.getLogger(getClass());

    public SequenceExecutorService(FeatureExecutorService featureExecutorService) {
        this.featureExecutorService = featureExecutorService;
    }

    /**
     * Calculate all features for the given sequence step, and wait until all results are available.
     */
    public void executeSequence(CalculationContext ctx, Integer currentSequence) {
        log(logger, ctx, "[S=%d] Executing sequence", currentSequence);

        // Note that requests are launched in parallel, because collecting input data may take some time.
        List<ScriptExecutionRequest> requests = ctx.getProtocolData().protocol.getFeatures().parallelStream()
        		.filter(f -> f.getSequence() == currentSequence)
        		.map(f -> featureExecutorService.executeFeature(ctx, f, currentSequence))
        		.filter(r -> r != null)
        		.toList();
        
        // Wait for all features to complete.
        requests.forEach(r -> { try { r.awaitOutput(); } catch (InterruptedException e) {} });

        log(logger, ctx, "[S=%s] All outputs received from script engine", currentSequence);
    }
}
