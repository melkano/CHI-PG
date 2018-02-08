/*
 * Copyright (C) 2015 Mikel Elkano Ilintxeta
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package es.unavarra.chi_pr.core;

import java.util.StringTokenizer;

/**
 * Provides the operations of fuzzy rules
 * @author Mikel Elkano Ilintxeta
 * @version 1.0
 */
public class FuzzyRule {
	
	private static int i;
	private static byte[] labels;
	private static String[] defuzzified;
	private static Variable[] variables;
    
    /**
     * Defuzzifies a fuzzy rule by returning the value with the highest membership degree for each antecedent
     * @param antecedents antecedents of the rule to be defuzzified
     * @return
     */
    public static String[] defuzzifyRule (byte[] antecedents){
    	
    	defuzzified = new String[antecedents.length];
    	variables = Mediator.getInputVariables();
    	
    	for (i = 0; i < antecedents.length; i++){
    		if (variables[i] instanceof NominalVariable)
    			defuzzified[i] = ((NominalVariable)variables[i])
					.getNominalValue(antecedents[i]);
    		else
    			defuzzified[i] = new Double(((FuzzyVariable)variables[i])
					.getFuzzySets()[antecedents[i]].getMidPoint()).toString();
    	}
    	
    	return defuzzified;
    	
    }
    
    /**
     * Returns a new rule represented by a byte array containing the index of antecedents and the class index (at last position of the array)
     * @param exampleStr input array representing the example
     * @return a new rule represented by a byte array containing the index of antecedents and the class index (at last position of the array)
     */
    public static byte[] getRuleFromExample (String[] exampleStr){
    	
        // Generate a new fuzzy rule
        Variable[] variables = Mediator.getInputVariables();
        labels = new byte[variables.length+1];
        // Get attributes
        for (i = 0; i < variables.length; i++)
        	labels[i] = variables[i].getLabelIndex(exampleStr[i]);
        // Get the class
        labels[variables.length] = Mediator.getClassIndex(exampleStr[variables.length]);
        
        return labels;
        
    }

}
