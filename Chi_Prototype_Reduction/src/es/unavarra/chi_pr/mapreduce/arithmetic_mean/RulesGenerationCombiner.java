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

package es.unavarra.chi_pr.mapreduce.arithmetic_mean;

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
import es.unavarra.chi_pr.utils.PartialSum;
import es.unavarra.chi_pr.utils.PartialSumsPairsWritable;

/**
 * Combiner class used to gather rules
 * @author Mikel Elkano Ilintxeta
 * @version 1.0
 */
public class RulesGenerationCombiner extends Reducer<ByteArrayWritable, PartialSumsPairsWritable, ByteArrayWritable, PartialSumsPairsWritable> {

	private Iterator<PartialSumsPairsWritable> iterator;
	private PartialSumsPairsWritable currentPartialSumPairs;
	
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
    public void reduce(ByteArrayWritable key, Iterable<PartialSumsPairsWritable> values, Context context) throws IOException, InterruptedException {
        
    	currentPartialSumPairs = new PartialSumsPairsWritable();
    	
    	iterator = values.iterator();

    	while (iterator.hasNext())
    		currentPartialSumPairs.add(iterator.next());
    	
    	/*
    	 * Key: Antecedents of the rule
    	 * Value: <numExamples, partialSums> pairs for each class
    	 */
    	context.write(key, currentPartialSumPairs);
        
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
		
	}
    
}

