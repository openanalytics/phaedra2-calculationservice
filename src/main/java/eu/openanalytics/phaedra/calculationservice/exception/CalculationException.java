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
package eu.openanalytics.phaedra.calculationservice.exception;

import eu.openanalytics.phaedra.calculationservice.model.Formula;
import eu.openanalytics.phaedra.protocolservice.dto.CalculationInputValueDTO;
import eu.openanalytics.phaedra.protocolservice.dto.FeatureDTO;
import eu.openanalytics.phaedra.protocolservice.dto.FeatureStatDTO;

public class CalculationException extends RuntimeException {

	private static final long serialVersionUID = -2296780206892476262L;

	public CalculationException(String msg) {
		super(msg);
	}

	public CalculationException(String msg, Object... args) {
		super(String.format(msg, args));
	}

	public static void doThrow(String msg, Object...args) {
		StringBuilder errorMessage = new StringBuilder();
    	errorMessage.append(msg);

    	for (Object arg: args) {
    		if (arg instanceof FeatureDTO) errorMessage.append(String.format(" [feature %s (%d)]", ((FeatureDTO) arg).getName(), ((FeatureDTO) arg).getId()));
    		if (arg instanceof Formula) errorMessage.append(String.format(" [formula %s (%d)]", ((Formula) arg).getName(), ((Formula) arg).getId()));
    		if (arg instanceof CalculationInputValueDTO) errorMessage.append(String.format(" [variable %s (%d)]", ((CalculationInputValueDTO) arg).getVariableName()));
    		if (arg instanceof FeatureStatDTO) errorMessage.append(String.format(" [stat %s (%d)]", ((FeatureStatDTO) arg).getName(), ((FeatureStatDTO) arg).getId()));
    	}

    	throw new CalculationException(errorMessage.toString());
	}
}
