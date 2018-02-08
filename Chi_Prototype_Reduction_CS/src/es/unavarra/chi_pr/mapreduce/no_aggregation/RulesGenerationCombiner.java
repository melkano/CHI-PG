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
import org.apache.hadoop.mapreduce.Reducer;

import es.unavarra.chi_pr.core.Mediator;
import es.unavarra.chi_pr.utils.ByteArrayWritable;
import es.unavarra.chi_pr.utils.LongArrayWritable;

/**
 * Combiner class used to gather rules
 * @author Mikel Elkano Ilintxeta
 * @version 1.0
 */
public class RulesGenerationCombiner extends Reducer<ByteArrayWritable, LongArrayWritable, ByteArrayWritable, LongArrayWritable> {

	private Iterator<LongArrayWritable> iterator;
	private LongArrayWritable currentNumEx;
	private long[] numExamples;
	private int i;
	
	private long startMs, endMs;
	
	@Override
	protected void cleanup (Context context) throws IOException, InterruptedException{
		
		// Write execution time
		endMs = System.currentTimeMillis();
		long mapperID = context.getTaskAttemptID().getTaskID().getId();
		try {
        	FileSystem fs = FileSystem.get(Mediator.getConfiguration());
        	Path file = new Path(Mediator.getHDFSLocation()+Mediator.getOutputPath()+Mediator.TIME_STATS_DIR+"/combiner"+mapperID+".txt");
        	OutputStream os = fs.create(file);
        	BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(os));
        	bw.write("Execution time (seconds): "+((endMs-startMs)/1000));
        	bw.close();
        	os.close();
        }
        catch(Exception e){
        	System.err.println("\nMAP PHASE: ERROR WRITING EXECUTION TIME");
			e.printStackTrace();
        }
		
	}
	
    @Override
    public void reduce(ByteArrayWritable key, Iterable<LongArrayWritable> values, Context context) throws IOException, InterruptedException {
        
    	// Restart the counter of number of examples
    	for (i = 0; i < numExamples.length; i++)
			numExamples[i] = 0;
    	
    	iterator = values.iterator();

    	while (iterator.hasNext()){
    		currentNumEx = iterator.next();
    		for (i = 0; i < numExamples.length; i++)
    			numExamples[i] += currentNumEx.getData()[i];
    	}
    	
    	/*
    	 * Key: Antecedents of the rule
    	 * Value: Number of examples for each class
    	 */
    	context.write(key, new LongArrayWritable(numExamples));
        
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
			System.err.println("\nMAP PHASE: ERROR READING CONFIGURATION: "+e.getMessage()+"\n");
			e.printStackTrace();
			System.exit(-1);
		}
		
		// Initialize the number of examples for each class
		numExamples = new long[Mediator.getNumClasses()];
	
	}
    
}

