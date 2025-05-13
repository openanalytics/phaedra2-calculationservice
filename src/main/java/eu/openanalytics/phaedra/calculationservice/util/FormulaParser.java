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
package eu.openanalytics.phaedra.calculationservice.util;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import eu.openanalytics.phaedra.calculationservice.enumeration.ScriptLanguage;
import eu.openanalytics.phaedra.calculationservice.execution.input.CalculationInputHelper;
import eu.openanalytics.phaedra.calculationservice.model.Formula;

public class FormulaParser {

	private static final Pattern R_INPUT_PATTERN = Pattern.compile("(?s).*?input\\$(\\w+).*");

	public List<String> parseInputNames(Formula formula) {
		return parseInputNames(formula.getFormula(), formula.getLanguage());
	}

	public List<String> parseInputNames(String formula, ScriptLanguage language) {
    	Set<String> names = new HashSet<>();

    	if (language  == ScriptLanguage.R) {
    		String bodyToParse = formula;
    		Matcher matcher = R_INPUT_PATTERN.matcher(bodyToParse);
    		while (matcher.matches()) {
    			String inputName = matcher.group(1);
    			if (!CalculationInputHelper.isReservedInputName(inputName)) {
    				names.add(inputName);
    			}
    			bodyToParse = bodyToParse.substring(matcher.end(1));
    			matcher = R_INPUT_PATTERN.matcher(bodyToParse);
    		}
    	}

    	return names.stream().sorted().toList();
	}

}
