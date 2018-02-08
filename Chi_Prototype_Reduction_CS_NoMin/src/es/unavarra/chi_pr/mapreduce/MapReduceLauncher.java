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

package es.unavarra.chi_pr.mapreduce;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import es.unavarra.chi_pr.core.Mediator;
import es.unavarra.chi_pr.mapreduce.no_aggregation.PrototypesGenerationReducer;
import es.unavarra.chi_pr.mapreduce.no_aggregation.RulesGenerationCombiner;
import es.unavarra.chi_pr.mapreduce.no_aggregation.RulesGenerationInMapperCombiner;
import es.unavarra.chi_pr.mapreduce.no_aggregation.RulesGenerationMapper;
import es.unavarra.chi_pr.utils.ByteArrayWritable;
import es.unavarra.chi_pr.utils.LongArrayWritable;
import es.unavarra.chi_pr.utils.PartialSumsPairsWritable;

/**
 * Launches the MapReduce application. Usage: hadoop jar <jar_file> es.unavarra.chi_pr.mapreduce.MapReduceLauncher <hdfs://url:port> <parameters_file> <header_file> <input_path> <output_path>
 * @author Mikel Elkano Ilintxeta
 * @version 1.0
 */
public class MapReduceLauncher {
	
	private static long startMs, endMs;
	
	private static final char[] VALID_OPTIONAL_ARGS = new char[]{};
	private static final char[] OPTIONAL_ARGS_DESCRIPTIONS = new char[]{};
    private static final int MAX_OPTIONAL_ARGS = VALID_OPTIONAL_ARGS.length;
    private static final int NUM_PROGRAM_ARGS = 5;
    private static final String COMMAND_STR = "hadoop jar <jar_file> es.unavarra.chi_pr.mapreduce.MapReduceLauncher"+printOptionalArgs()+"<hdfs://url:port> <parameters_file> <header_file> <input_path> <output_path>\n"+printOptionalArgsDescriptions();
    
    /**
     * Returns true if the specified arguments contains a given argument
     * @param args arguments
     * @param arg argument
     * @return true if the specified arguments contains a given argument
     */
    private static boolean containsArg (String[] args, String arg){
        for (int i = 0; i < args.length; i++){
            if (args[i].charAt(0) == '-' && args[i].contains(arg))
            	return true;
        }
        return false;
    }
    
    /**
     * Prepares and runs the MapReduce job to generate the prototypes
     * @throws IOException 
     * @throws InterruptedException 
     * @throws ClassNotFoundException 
     */
    private static void generatePrototypes() throws IOException, ClassNotFoundException, InterruptedException{
    	
    	Configuration conf = Mediator.getConfiguration();
        
        /*
         * Prepare and run the job
         */
        Job job = Job.getInstance(conf);

        job.setJarByClass(MapReduceLauncher.class);
        
        // NO AGGREGATION
        if (Mediator.getAggregation() == Mediator.AGGREGATION_NONE){
	        job.setMapperClass(es.unavarra.chi_pr.mapreduce.no_aggregation.RulesGenerationMapper.class);
	        job.setCombinerClass(es.unavarra.chi_pr.mapreduce.no_aggregation.RulesGenerationCombiner.class);
	        //job.setMapperClass(es.unavarra.chi_pr.mapreduce.no_aggregation.RulesGenerationInMapperCombiner.class);
	        job.setReducerClass(es.unavarra.chi_pr.mapreduce.no_aggregation.PrototypesGenerationReducer.class);
	        job.setMapOutputKeyClass(ByteArrayWritable.class);
	        job.setMapOutputValueClass(LongArrayWritable.class);
        }
        // ARITHMETIC MEAN
        else if (Mediator.getAggregation() == Mediator.AGGREGATION_ARITHMETIC_MEAN){
	        job.setMapperClass(es.unavarra.chi_pr.mapreduce.arithmetic_mean.RulesGenerationMapper.class);
	        job.setCombinerClass(es.unavarra.chi_pr.mapreduce.arithmetic_mean.RulesGenerationCombiner.class);
	        job.setReducerClass(es.unavarra.chi_pr.mapreduce.arithmetic_mean.multiple_classes.PrototypesGenerationReducer.class);
	        job.setMapOutputKeyClass(ByteArrayWritable.class);
	        job.setMapOutputValueClass(PartialSumsPairsWritable.class);
        }
        // ARITHMETIC MEAN SINGLE CLASS
        else if (Mediator.getAggregation() == Mediator.AGGREGATION_ARITHMETIC_MEAN_SINGLE_CLASS){
	        job.setMapperClass(es.unavarra.chi_pr.mapreduce.arithmetic_mean.RulesGenerationMapper.class);
	        job.setCombinerClass(es.unavarra.chi_pr.mapreduce.arithmetic_mean.RulesGenerationCombiner.class);
	        job.setReducerClass(es.unavarra.chi_pr.mapreduce.arithmetic_mean.single_class.PrototypesGenerationReducer.class);
	        job.setMapOutputKeyClass(ByteArrayWritable.class);
	        job.setMapOutputValueClass(PartialSumsPairsWritable.class);
        }
        // INVALID AGGREGATION
        else{
        	System.err.println("ERROR READING CONFIGURATION FILE: Aggregation not found\n");
        	System.exit(-1);
        }
        
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        /*SequenceFileOutputFormat.setCompressOutput(job, true);
        SequenceFileOutputFormat.setOutputCompressorClass(job, BZip2Codec.class);
        SequenceFileOutputFormat.setOutputCompressionType(job, CompressionType.BLOCK);*/
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        
        FileInputFormat.addInputPath(job, new Path(Mediator.getHDFSLocation()+Mediator.getInputPath()));
        FileInputFormat.setMaxInputSplitSize(job, Mediator.computeHadoopSplitSize(
    		Mediator.getHDFSLocation()+Mediator.getInputPath(), Mediator.getHadoopNumMappers()));
        FileInputFormat.setMinInputSplitSize(job, Mediator.computeHadoopSplitSize(
    		Mediator.getHDFSLocation()+Mediator.getInputPath(), Mediator.getHadoopNumMappers()));
        FileOutputFormat.setOutputPath(job, new Path(Mediator.getHDFSLocation()+Mediator.getOutputPath()+"_TMP"));
        
        job.setNumReduceTasks(Mediator.getHadoopNumReducers());
        
        job.waitForCompletion(true);
    	
    }
    
    /**
	 * Main method
	 * @author Mikel Elkano Ilintxeta
	 * @param args command line arguments
	 * @version 1.0
	 */
    public static void main(String[] args) {
    	
    	startMs = System.currentTimeMillis();
    	
    	/**
    	 * READ ARGUMENTS
    	 */

    	Configuration conf = new Configuration();
    	String[] otherArgs = null;
    	try{
	    	otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        
    	}
    	catch(Exception e){
    		System.err.println("\nINITIALIZATION ERROR:\n\n");
    		System.exit(-1);
    	}

    	String[] programArgs = new String[NUM_PROGRAM_ARGS];
        String[] optionalArgs = null;
        
        if (otherArgs.length < NUM_PROGRAM_ARGS || otherArgs.length > (MAX_OPTIONAL_ARGS + NUM_PROGRAM_ARGS)){
            System.err.println("\nUsage: "+COMMAND_STR+"\n");
            System.exit(2);
        }
        if (otherArgs.length > NUM_PROGRAM_ARGS){
            int numOptionalArgs = otherArgs.length - NUM_PROGRAM_ARGS;
            optionalArgs = new String[numOptionalArgs];
            for (int i = 0; i < numOptionalArgs; i++)
                optionalArgs[i] = otherArgs[i];
            for (int i = numOptionalArgs; i < otherArgs.length; i++)
                programArgs[i - otherArgs.length + NUM_PROGRAM_ARGS] = otherArgs[i];
            // Check optional args
            if (!validOptionalArgs(optionalArgs)){
                System.err.println("\nUsage: "+COMMAND_STR+"\n");
                System.exit(2);
            }
        }
        else
            programArgs = otherArgs;
        
        //boolean verbose = (optionalArgs==null)?false:containsArg(optionalArgs,"v");
        String hdfsLocation = programArgs[0];
        String paramsPath = programArgs[1];
        String headerPath = programArgs[2];
        String inputPath = programArgs[3];
        String outputPath = programArgs[4];
        
        /**
         * SAVE BASIC PARAMETERS
         */
    	Mediator.setConfiguration(conf);
        Mediator.saveHDFSLocation(hdfsLocation);
        Mediator.saveInputPath(inputPath);
        Mediator.saveOutputPath(outputPath);
        
        /**
         * READ CONFIGURATION FILE
         */
        try{
        	Mediator.storeConfigurationParameters(hdfsLocation+"/"+paramsPath);
        }
        catch(Exception e){
        	System.err.println("ERROR READING CONFIGURATION FILE:\n");
        	e.printStackTrace();
        	System.exit(-1);
        }
        
        /**
         * READ HADOOP CONFIGURATION
         */
        try {
        	Mediator.readHadoopConfiguration();
        }
        catch(Exception e){
        	System.err.println("ERROR READING HADOOP CONFIGURATION:\n");
        	e.printStackTrace();
        	System.exit(-1);
        }
        
        /**
         * READ THE HEADER FILE AND CREATE THE DATA BASE
         */
        try{
        	Mediator.readHeaderFile(hdfsLocation+"/"+headerPath);
        }
        catch(Exception e){
        	System.err.println("ERROR READING HEADER FILE:\n");
        	e.printStackTrace();
        	System.exit(-1);
        }
        
        // Read configuration
        try{
        	Mediator.readConfiguration();
        }
        catch(Exception e){
        	System.err.println("ERROR READING CONFIGURATION: "+e.getMessage()+"\n");
        	e.printStackTrace();
        	System.exit(-1);
        }
        
        // Check number of classes
        if (Mediator.getNumClasses() > 127){
        	System.err.println("\nERROR: The maximum number of classes is 127\n");
        	System.err.println(-1);
        }
    	
        /**
         * GENERATE PROTOTYPES
         */
    	try {
    		generatePrototypes();
    	}
    	catch(Exception e){
    		System.err.println("\nERROR GENERATING PROTOTYPES:\n");
    		e.printStackTrace();
    		System.exit(-1);
    	}
    	
    	/**
    	 * PRINT USED PARAMETERS
    	 */
    	String aggStr = "NONE";
    	if (Mediator.getAggregation()==Mediator.AGGREGATION_ARITHMETIC_MEAN)
    		aggStr = "arithmetic mean";
    	else if (Mediator.getAggregation()==Mediator.AGGREGATION_ARITHMETIC_MEAN_SINGLE_CLASS)
    		aggStr = "arithmetic mean (single class)";
    	System.out.println("\nPrototypes built. Parameters:\n");
    	System.out.println("\tNumber of linguistic labels: "+Mediator.getNumLinguisticLabels());
    	System.out.println("\tAggregation: "+aggStr);
    	System.out.println("\tNumber of mappers: "+Mediator.getHadoopNumMappers());
    	System.out.println("\tNumber of reducers: "+Mediator.getHadoopNumReducers());
    	System.out.println("\nWriting to disk...");
    	
    	/**
    	 * MERGE PROTOTYPES FROM ALL REDUCERS
    	 */
    	try {
			mergePrototypes();
    	}
    	catch(Exception e){
    		System.err.println("\nERROR WRITING PROTOTYPES\n");
    		e.printStackTrace();
    	}
    	
    	/**
    	 * WRITE EXECUTION TIME
    	 */
    	writeExecutionTime();
    	
    	System.out.println("Done.\n\n");
    	        
    }
    
    /**
     * Merges the prototypes generated in the reducers
     * @throws IOException 
     */
    private static void mergePrototypes() throws IOException {
    	
    	FileSystem fs = FileSystem.get(Mediator.getConfiguration());
    	FileStatus[] status = fs.listStatus(new Path(Mediator.getHDFSLocation()+Mediator.getOutputPath()+"_TMP"));
		BufferedWriter bw = new BufferedWriter(
			new OutputStreamWriter(fs.create(
			new Path(Mediator.getHDFSLocation()+Mediator.getOutputPrototypesPath()),true)));
		Reader reader;
		Text values = new Text();
		NullWritable nullValue = NullWritable.get();
    	
    	// Read all sequence files
    	for (FileStatus fileStatus:status){
    		
    		if (!fileStatus.getPath().getName().contains("_SUCCESS")){
    		
	    		reader = new Reader(Mediator.getConfiguration(), Reader.file(fileStatus.getPath()));

	            while (reader.next(values, nullValue)){
	            	bw.write(values+"\n");
	            }
	            
	            reader.close();
	            
    		}
    		
    	}
    	
    	bw.close();
    	
    	// Remove input path
    	fs.delete(new Path(Mediator.getHDFSLocation()+Mediator.getOutputPath()+"_TMP"),true);
    	
    }
    
    /**
     * Returns the list of optional args
     * @return list of optional args
     */
    private static String printOptionalArgs() {
        String output = "";
        for (int i = 0; i < MAX_OPTIONAL_ARGS; i++)
            output += " [-" + VALID_OPTIONAL_ARGS[i] + "]";
        return output + " ";
    }
    
    /**
     * Returns the description of optional args
     * @return description of optional args
     */
    private static String printOptionalArgsDescriptions() {
    	if (MAX_OPTIONAL_ARGS > 0) {
	        String output = "\nOptional arguments:\n\n";
	        for (int i = 0; i < MAX_OPTIONAL_ARGS; i++)
	            output += "\t-" + VALID_OPTIONAL_ARGS[i] + ": "+OPTIONAL_ARGS_DESCRIPTIONS[i]+"\n";
	        return output;
	    	}
    	else
    		return "";
    }
    
    /**
     * Returns true if the specified optional arguments are valid
     * @param args optional arguments
     * @return true if the specified optional arguments are valid
     */
    private static boolean validOptionalArgs (String[] args){
        boolean validChar;
        // Iterate over args
        for (int i = 0; i < args.length; i++){
            if (args[i].charAt(0)!='-')
            	return false;
            // Iterate over characters (to read combined arguments)
            for (int charPos = 1; charPos < args[i].length(); charPos++){
            	validChar = false;
            	for (int j = 0; j < VALID_OPTIONAL_ARGS.length; j++){
	                if (args[i].charAt(charPos) == VALID_OPTIONAL_ARGS[j]){
	                    validChar = true;
	                    break;
	                }
            	}
            	if (!validChar)
            		return false;
        	}
        }
        return true;
    }
    
    /**
     * Writes total execution time
     */
    private static void writeExecutionTime() {
    	
    	endMs = System.currentTimeMillis();
    	long elapsed = endMs - startMs;
    	long hours = elapsed / 3600000;
        long minutes = (elapsed % 3600000) / 60000;
        long seconds = ((elapsed % 3600000) % 60000) / 1000;
        
        try {
        	
        	FileSystem fs = FileSystem.get(Mediator.getConfiguration());
        	Path timeDirPath = new Path(Mediator.getHDFSLocation()+Mediator.getOutputPath()+Mediator.TIME_STATS_DIR);
        	Path timeFilePath = new Path(Mediator.getHDFSLocation()+Mediator.getOutputPath()+"time.txt");
        	OutputStream os = fs.create(timeFilePath);
        	BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(os));
        	
        	/**
        	 * Write total execution time
        	 */
        	bw.write("Total execution time (hh:mm:ss): "+String.format("%02d",hours)+":"+String.format("%02d",minutes)+":"+
    			String.format("%02d",seconds)+" ("+(elapsed/1000)+" seconds)\n");
        	
        	/**
        	 *  Write Mappers/Combiners/Reducers execution time avg.
        	 */
        	FileStatus[] status = fs.listStatus(timeDirPath);
        	BufferedReader br = null;
        	String buffer;
        	long sumMappers = 0, sumCombiners = 0, sumReducers = 0, 
        			numMappers = 0, numCombiners = 0, numReducers = 0;
        	for (FileStatus fileStatus:status){
        		// Read Mappers time
        		if (fileStatus.getPath().getName().contains("mapper")){
        			numMappers ++;
        			br=new BufferedReader(new InputStreamReader(fs.open(fileStatus.getPath())));
        			buffer = br.readLine();
        			sumMappers += Long.parseLong(buffer.substring(buffer.indexOf(":")+1).trim());
        		}
        		// Read Combiners time
        		else if (fileStatus.getPath().getName().contains("combiner")){
        			numCombiners ++;
        			br=new BufferedReader(new InputStreamReader(fs.open(fileStatus.getPath())));
        			buffer = br.readLine();
        			sumCombiners += Long.parseLong(buffer.substring(buffer.indexOf(":")+1).trim());
        		}
        		// Read Reducers time
        		else if (fileStatus.getPath().getName().contains("reducer")){
        			numReducers ++;
        			br=new BufferedReader(new InputStreamReader(fs.open(fileStatus.getPath())));
        			buffer = br.readLine();
        			sumReducers += Long.parseLong(buffer.substring(buffer.indexOf(":")+1).trim());
        		}
        		br.close();
        	}
        	
        	// Write Mappers avg. runtime
        	elapsed = sumMappers / numMappers;
        	hours = elapsed / 3600;
            minutes = (elapsed % 3600) / 60;
            seconds = (elapsed % 3600) % 60;
            bw.write("Mappers ("+ numMappers+") avg. execution time (hh:mm:ss): "+String.format("%02d",hours)+":"+String.format("%02d",minutes)+":"+
        			String.format("%02d",seconds)+" ("+elapsed+" seconds)\n");
            
            // Write Combiners avg. runtime
            if (numCombiners > 0){
	            elapsed = sumCombiners / numCombiners;
	        	hours = elapsed / 3600;
	            minutes = (elapsed % 3600) / 60;
	            seconds = (elapsed % 3600) % 60;
	            bw.write("Combiners ("+ numCombiners+") avg. execution time (hh:mm:ss): "+String.format("%02d",hours)+":"+String.format("%02d",minutes)+":"+
	        			String.format("%02d",seconds)+" ("+elapsed+" seconds)\n");
            }
            
            // Write Reducers avg. runtime
            elapsed = sumReducers / numReducers;
        	hours = elapsed / 3600;
            minutes = (elapsed % 3600) / 60;
            seconds = (elapsed % 3600) % 60;
            bw.write("Reducers ("+ numReducers+") avg. execution time (hh:mm:ss): "+String.format("%02d",hours)+":"+String.format("%02d",minutes)+":"+
        			String.format("%02d",seconds)+" ("+elapsed+" seconds)\n");
        	
        	bw.close();
        	os.close();
        	
        	// Remove time stats directory
        	fs.delete(timeDirPath,true);
        	
        }
        catch(Exception e){
        	System.err.println("\nERROR WRITING EXECUTION TIME:\n");
			e.printStackTrace();
        }
    	
    }
    
}
