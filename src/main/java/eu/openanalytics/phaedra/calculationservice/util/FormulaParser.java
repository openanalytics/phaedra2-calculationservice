package eu.openanalytics.phaedra.calculationservice.util;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import eu.openanalytics.phaedra.calculationservice.enumeration.ScriptLanguage;
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
    			names.add(matcher.group(1));
//    			System.out.println("Match found at " + matcher.start() + " - " + matcher.end(1) + ": " + matcher.group(1));
    			bodyToParse = bodyToParse.substring(matcher.end(1));
    			matcher = R_INPUT_PATTERN.matcher(bodyToParse);
    		}
    	}

    	return names.stream().sorted().toList();
	}

}
