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

package es.unavarra.chi_pr.mapreduce.no_aggregation;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.Iterator;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import es.unavarra.chi_pr.core.FuzzyRule;
import es.unavarra.chi_pr.core.Mediator;
import es.unavarra.chi_pr.utils.ByteArrayWritable;
import es.unavarra.chi_pr.utils.LongArrayWritable;

/**
 * Reducer class that generates a new prototype from a rule
 * @author Mikel Elkano Ilintxeta
 * @version 1.0
 */
public class PrototypesGenerationReducer extends Reducer<ByteArrayWritable, LongArrayWritable, Text, NullWritable> {

	private int i;
	private byte majClass;
	private double maxProportion, proportion;
	private long[] numExamples;
	private Iterator<LongArrayWritable> iterator;
	private LongArrayWritable currentNumExamples;
	private String outputStr;
	private String[] defuzzifiedRule;
	
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
	 * Returns the class with the highest number of examples
	 * @param numExamples array containing the number of examples for each class
	 * @return class with the highest number of examples
	 */
	private void getMajorityClass(long[] numExamples){
		
		majClass = -1;
		maxProportion = 0.0;
		
		for (i = 0; i < numExamples.length; i++){
			proportion = numExamples[i] / Mediator.getClassNumExamples()[i];
			if (proportion > maxProportion){
				maxProportion = proportion;
				majClass = (byte)i;
			}
		}
		
		// Check if the number of instances of the majority class is higher than the minimum threshold
		if (numExamples[majClass] < Mediator.getPrototypeMinNumExamples(majClass))
			majClass = -1;
		
	}
	
    @Override
    public void reduce(ByteArrayWritable key, Iterable<LongArrayWritable> values, Context context) throws IOException, InterruptedException {
        
    	for (i = 0; i < numExamples.length; i++)
			numExamples[i] = 0;
    	
    	iterator = values.iterator();
    	
    	// Sum the number of examples for each class
    	while (iterator.hasNext()){
    		currentNumExamples = iterator.next();
    		for (i = 0; i < numExamples.length; i++)
    			numExamples[i] += currentNumExamples.getData()[i];
    	}
    	
    	// Select the class with highest proportion of number of examples
    	getMajorityClass(numExamples);
    	if (majClass > -1){
    	
	    	// Write a new prototype 
			/*
	    	 * Key: Instance values
	    	 * Value: Class value
	    	 */
    		defuzzifiedRule = FuzzyRule.defuzzifyRule(key.getBytes());
    		outputStr = "";
    		for (i = 0; i < Mediator.getNumInputVariables(); i++)
    			outputStr += defuzzifiedRule[i] + ",";
    		outputStr += Mediator.getClassLabel(majClass);
			context.write(new Text(outputStr), NullWritable.get());
		
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
		
		// Initialize the number of examples for each class
		numExamples = new long[Mediator.getNumClasses()];
	
	}
    
}

