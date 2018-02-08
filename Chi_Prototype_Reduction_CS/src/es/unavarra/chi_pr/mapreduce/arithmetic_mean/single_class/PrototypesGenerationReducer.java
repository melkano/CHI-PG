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

package es.unavarra.chi_pr.mapreduce.arithmetic_mean.single_class;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import es.unavarra.chi_pr.core.FuzzyVariable;
import es.unavarra.chi_pr.core.Mediator;
import es.unavarra.chi_pr.core.NominalVariable;
import es.unavarra.chi_pr.utils.ByteArrayWritable;
import es.unavarra.chi_pr.utils.PartialSumsPairsWritable;

/**
 * Reducer class that generates a new prototype from a rule
 * @author Mikel Elkano Ilintxeta
 * @version 1.0
 */
public class PrototypesGenerationReducer extends Reducer<ByteArrayWritable, PartialSumsPairsWritable, Text, NullWritable> {

	private int currentClass, mostFreqClass;
	private int j, k;
	private long modeValue;
	private long currentMode;
	private int modeIndex;
	private String prototype;
	private Iterator<PartialSumsPairsWritable> iterator;
	private PartialSumsPairsWritable currentSumPairs;
	private double proportion,maxProportion;
	
	private long startMs, endMs;
	
	@Override
	protected void cleanup (Context context) throws IOException, InterruptedException{
		
		// Write execution time
		endMs = System.currentTimeMillis();
		long reducerID = context.getTaskAttemptID().getTaskID().getId();
		try {
        	FileSystem fs = FileSystem.get(Mediator.getConfiguration());
        	Path file = new Path(Mediator.getHDFSLocation()+Mediator.getOutputPath()+Mediator.TIME_STATS_DIR+"/reducer"+reducerID+".txt");
        	OutputStream os = fs.create(file);
        	BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(os));
        	bw.write("Execution time (seconds): "+((endMs-startMs)/1000));
        	bw.close();
        	os.close();
        }
        catch(Exception e){
        	System.err.println("\nREDUCE PHASE: ERROR WRITING EXECUTION TIME");
			e.printStackTrace();
        }
		
	}
	
	/**
	 * Computes the most frequent class in the current partition (with a minimum number of examples)
	 * @param numInstances array containing the number of instances of each class
	 */
	private void computeMostFrequentClass(long[] numInstances){
		
		mostFreqClass = -1;
		maxProportion = 0.0;
		
		for (currentClass = 0; currentClass < numInstances.length; currentClass++){
			proportion = (double)numInstances[currentClass] / (double)Mediator.getClassNumExamples()[currentClass];
			if (proportion > maxProportion){
				maxProportion = proportion;
				mostFreqClass = currentClass;
			}
		}
		
		// Check if the number of instances of the majority class is higher than the minimum threshold
		if (numInstances[mostFreqClass] < Mediator.getPrototypeMinNumExamples(mostFreqClass))
			mostFreqClass = -1;
		
	}
	
    @Override
    public void reduce(ByteArrayWritable key, Iterable<PartialSumsPairsWritable> values, Context context) throws IOException, InterruptedException {
        
    	currentSumPairs = new PartialSumsPairsWritable();
    	
    	iterator = values.iterator();

    	while (iterator.hasNext())
    		currentSumPairs.add(iterator.next());
    	
    	// Build and write a prototype for the most frequent class (with a minimum number of examples)
		computeMostFrequentClass(currentSumPairs.getNumExamples());
		if (mostFreqClass > -1){
			prototype = "";
			for (j = 0; j < Mediator.getNumInputVariables(); j++){
				// For nominal values, compute the mode
				if (Mediator.getInputVariables()[j] instanceof NominalVariable){
					// Get the mode
					modeValue = 0;
					modeIndex = 0;
					for (k = 0; k < ((NominalVariable)Mediator.getInputVariables()[j]).getNominalValues().length; k++){
						currentMode = currentSumPairs.getPartialSums()[mostFreqClass][j].getCategoricalSums()[k];
						if (currentMode > modeValue){
							modeValue = currentMode;
							modeIndex = k;
						}
					}
					prototype += ((NominalVariable)Mediator.getInputVariables()[j]).getNominalValues()[modeIndex] + ",";
				}
				// For numerical values, compute the arithmetic mean
				else{
					if (((FuzzyVariable)Mediator.getInputVariables()[j]).getValuesType() == FuzzyVariable.TYPE_REAL)
						prototype += Double.valueOf(currentSumPairs.getPartialSums()[mostFreqClass][j].getNumericSum() / currentSumPairs.getNumExamples()[mostFreqClass]).toString() + ",";
					else
						prototype += Long.valueOf(Math.round(currentSumPairs.getPartialSums()[mostFreqClass][j].getNumericSum() / currentSumPairs.getNumExamples()[mostFreqClass])).toString() + ",";
				}
			}
			prototype += Mediator.getClassLabel((byte)mostFreqClass);
			context.write(new Text(prototype), NullWritable.get());
		}
    }
    
    @Override
	protected void setup(Context context) throws InterruptedException, IOException{

		super.setup(context);
		
		startMs = System.currentTimeMillis();
		
		try {
			Mediator.setConfiguration(context.getConfiguration());
			Mediator.readConfiguration();
		}
		catch(Exception e){
			System.err.println("\nREDUCE PHASE: ERROR READING CONFIGURATION: "+e.getMessage()+"\n");
			e.printStackTrace();
			System.exit(-1);
		}
	
	}
    
}

