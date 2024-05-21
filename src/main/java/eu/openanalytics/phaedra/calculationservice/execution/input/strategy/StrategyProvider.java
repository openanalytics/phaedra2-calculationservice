package eu.openanalytics.phaedra.calculationservice.execution.input.strategy;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import eu.openanalytics.phaedra.calculationservice.execution.CalculationContext;
import eu.openanalytics.phaedra.protocolservice.dto.FeatureDTO;

@Service
public class StrategyProvider {

	@Autowired
	private List<InputGroupingStrategy> strategies;
	
	public InputGroupingStrategy getStrategy(CalculationContext ctx, FeatureDTO feature) {
		for (InputGroupingStrategy strat: strategies) {
			if (strat.isSuited(ctx, feature)) {
				return strat;
			}
		}
		return null;
	}
}
