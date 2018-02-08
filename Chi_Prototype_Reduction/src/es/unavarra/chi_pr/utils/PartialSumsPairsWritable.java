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

package es.unavarra.chi_pr.utils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

import org.apache.hadoop.io.Writable;

import es.unavarra.chi_pr.core.Mediator;
import es.unavarra.chi_pr.core.NominalVariable;

/**
 * Serializable object containing the number of examples and the sum of each feature for each class
 * @author Mikel Elkano Ilintxeta
 * @version 1.0
 */
public class PartialSumsPairsWritable implements Writable, Serializable {
	
	private long[] numExamples;
	private PartialSum[][] sums;
	private long[] tmpCatSums;
	private int numCatValues;
	
	private int numClasses;
	
	private PartialSumsPairsWritable o;
	
	private int i, j, k;
	
	/**
     * Default constructor (creates zero pairs)
     */
    public PartialSumsPairsWritable() {
    	this.numClasses = Mediator.getNumClasses();
    	this.numExamples = new long[this.numClasses];
    	for (i = 0; i < this.numClasses; i++)
    		this.numExamples[i] = 0;
    	this.sums = new PartialSum[this.numClasses][Mediator.getNumInputVariables()];
    	for (i = 0; i < this.numClasses; i++)
    		for (j = 0; j < Mediator.getNumInputVariables(); j++)
    			if (Mediator.getInputVariables()[j] instanceof NominalVariable){
    				numCatValues = ((NominalVariable)Mediator.getInputVariables()[j]).getNominalValues().length;
        			tmpCatSums = new long[numCatValues];
        			for (k = 0; k < numCatValues; k++)
        				tmpCatSums[k] = 0;
        			this.sums[i][j] = new PartialSum(tmpCatSums);
    			}
    			else
    				this.sums[i][j] = new PartialSum(0.0);
    }
    
    /**
     * 
     * @param numExamples array containing the number of examples for each class
     * @param sums array containing the partial sum of each feature for each class
     * @throws Exception
     */
    public PartialSumsPairsWritable(long[] numExamples, PartialSum[][] sums) {

    	this.numClasses = numExamples.length;
    	this.numExamples = numExamples;
    	this.sums = sums;
    }
    
    @Override
	public boolean equals (Object obj){
    	
    	if (obj == this)
    		return true;
    	if (obj == null || obj.getClass() != this.getClass())
    		return false;
    	
    	o = (PartialSumsPairsWritable)obj;
    	if (numClasses == o.numClasses){
			for (i = 0; i < numClasses; i++){
				if (numExamples[i] != o.numExamples[i])
					return false;
				for (j = 0; j < Mediator.getNumInputVariables(); j++)
					if (!sums[i][j].equals(o.sums[i][j]))
						return false;
			}
			return true;
		}
    	else
    		return false;
    	
    }
    
    /**
     * Returns the number of examples for each class
     * @return number of examples for each class
     */
    public long[] getNumExamples(){
    	return numExamples;
    }
    
    /**
     * Returns the partial sum of each feature for each class
     * @return partial sum of each feature for each class
     */
    public PartialSum[][] getPartialSums(){
    	return sums;
    }
    
    @Override
    public void readFields(DataInput in) throws IOException {
    	
        numClasses = in.readInt();

        numExamples = new long[numClasses];
        sums = new PartialSum[numClasses][Mediator.getNumInputVariables()];

        for(i = 0; i < numClasses; i++)
            numExamples[i] = in.readLong();
        
        for(i = 0; i < numClasses; i++){
        	if (numExamples[i] > 0){
	        	for(j = 0; j < Mediator.getNumInputVariables(); j++){
	        		if (Mediator.getInputVariables()[j] instanceof NominalVariable){
	        			numCatValues = ((NominalVariable)Mediator.getInputVariables()[j]).getNominalValues().length;
	        			tmpCatSums = new long[numCatValues];
	        			for (k = 0; k < numCatValues; k++)
	        				tmpCatSums[k] = in.readLong();
	        			sums[i][j] = new PartialSum(tmpCatSums);
	        		}
	        		else
	        			sums[i][j] = new PartialSum(in.readDouble());
	        	}
        	}
        }
        
    }
    
    /**
     * Adds the given partial sums pairs to the current ones
     * @param partialSumsPairs partial sums pairs
     */
    public void add (PartialSumsPairsWritable partialSumsPairs) {
    	for(i = 0; i < numClasses; i++){
            numExamples[i] += partialSumsPairs.numExamples[i];
    		for(j = 0; j < Mediator.getNumInputVariables(); j++)
    			sums[i][j].sum(partialSumsPairs.sums[i][j]);
    	}
    }
    
    /*
    @Override
    public String toString(){
    	
    	String output = "<numExamples, partialSum> for each class: ";
    	
    	for (i = 0; i < numClasses; i++)
    		output += "<"+numExamples[i] +", " + sums[i] + "> | ";
    	
    	return output;
    	
    }*/
    
    @Override
    public void write(DataOutput out) throws IOException {

        out.writeInt(numClasses);

        for(i = 0; i < numClasses; i++)
            out.writeLong(numExamples[i]);
        
        for(i = 0; i < numClasses; i++){
        	if (numExamples[i] > 0){
	        	for(j = 0; j < Mediator.getNumInputVariables(); j++){
	        		if (Mediator.getInputVariables()[j] instanceof NominalVariable)
	        			for (k = 0; k < sums[i][j].getCategoricalSums().length; k++)
	        				out.writeLong(sums[i][j].getCategoricalSums()[k]);
	        		else
	        			out.writeDouble(sums[i][j].getNumericSum());
	        	}
        	}
        }
        
    }

}
