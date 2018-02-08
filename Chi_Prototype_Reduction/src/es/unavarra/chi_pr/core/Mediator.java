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

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.sun.org.apache.xml.internal.security.exceptions.Base64DecodingException;
import com.sun.org.apache.xml.internal.security.utils.Base64;

/**
 * Class containing all global objects and methods (this class is the only accessing configuration file)
 * @author Mikel Elkano Ilintxeta
 * @version 1.0
 */
public class Mediator {
	
	/**
	 * Aggregation field
	 */
	private static final String AGGREGATION_FIELD = "aggregation";
	
	/**
	 * Aggregations
	 */
	public static final byte AGGREGATION_NONE = 0, AGGREGATION_ARITHMETIC_MEAN = 1, AGGREGATION_ARITHMETIC_MEAN_SINGLE_CLASS = 2;
	
	/**
	 * Class labels field
	 */
	private static final String CLASS_LABELS_FIELD = "class_labels";
	
	/**
	 * Number of examples of each class field
	 */
	private static final String CLASS_NUM_EXAMPLES_FIELD = "class_num_examples";
	
	/**
	 * Hadoop: maximum minutes with no status update in mapreduce tasks
	 */
	private static final String HADOOP_MAX_MINS_NO_UPDATE_FIELD = "hadoop_max_mins_no_update";
	
	/**
	 * Hadoop: number of mappers
	 */
	private static final String HADOOP_NUM_MAPPERS_FIELD = "hadoop_num_mappers";
	
	/**
	 * Hadoop: number of reducers
	 */
	private static final String HADOOP_NUM_REDUCERS_FIELD = "hadoop_num_reducers";
	
	/**
	 * HDFS location field
	 */
	private static final String HDFS_LOCATION_FIELD = "hdfs_location";
	
	/**
	 * Input path field
	 */
	private static final String INPUT_PATH_FIELD = "input_path";
	
	/**
	 * Number of classes field
	 */
	private static final String NUM_CLASSES_FIELD = "num_classes";
	
	/**
	 * Number of linguistic labels field
	 */
	private static final String NUM_LINGUISTIC_LABELS_FIELD = "num_linguistic_labels";
	
	/**
	 * Number of variables field
	 */
	private static final String NUM_VARIABLES_FIELD = "num_variables";
	
	/**
	 * Output path field
	 */
	private static final String OUTPUT_PATH_FIELD = "output_path";
	
	/**
	 * Minimum number of examples per prototype field
	 */
	private static final String PROTO_MIN_NUM_EX_FIELD = "prototype_min_num_examples";
	
	/**
	 * Path for time stats
	 */
	public static final String TIME_STATS_DIR = "time";
	
	/**
	 * Variables field
	 */
	private static final String VARIABLES_FIELD = "variables";
	
	/**
     * Aggregation used to generate prototypes
     */
    private static byte aggregation;
	
	/**
     * Class labels
     */
    private static String[] classLabels;
    
    /**
     * Number of examples of each class
     */
    private static long[] classNumExamples;
    
    /**
     * Configuration object
     */
    private static Configuration configuration;
    
    /**
     * Hadoop: maximum minutes with no update
     */
    private static int hadoopMaxMinsNoUpdate;
    
    /**
     * Hadoop: number of mappers
     */
    private static int hadoopNumMappers;
    
    /**
     * Hadoop: number of reducers
     */
    private static int hadoopNumReducers;
	
	/**
     * HDFS Location
     */
    private static String hdfsLocation;
    
    /**
     * Input directory path
     */
    private static String inputPath;
    
    /**
     * Number of class labels
     */
    private static byte numClassLabels = 0;
	
	/**
     * Number of linguistic labels (fuzzy sets) considered for all fuzzy variables
     */
    private static byte numLinguisticLabels = 0;
    
    /**
     * Number of variables
     */
    private static int numVariables = 0;
    
    /**
     * Output directory path
     */
    private static String outputPath;
    
    /**
     * Minimum number of examples per prototype
     */
    private static int protoMinNumEx;
    
    /**
     * Variables of the problem
     */
    private static Variable[] variables;
    
    /**
     * Returns the needed split size to execute the specified number of mappers for the given input path
     * @param inputPath input path
     * @param numMappers number of mappers
     * @return needed split size to execute the specified number of mappers for the given input path
     * @throws IOException 
     */
    public static long computeHadoopSplitSize (String inputPath, int numMappers) throws IOException{
    	Path path = new Path(inputPath);
        FileSystem hdfs = path.getFileSystem(configuration);
        ContentSummary cSummary = hdfs.getContentSummary(path);
        return (cSummary.getLength() / numMappers);
    }
    
    /**
     * Returns the aggregation used to generate prototypes
     * @return aggregation used to generate prototypes
     */
    public static byte getAggregation(){
    	return aggregation;
    }
    
    /**
     * Returns class index
     * @param classLabel class label
     * @return class index
     */
    public static byte getClassIndex (String classLabel){
    	byte classIndex = -1;
        for (byte index = 0; index < classLabels.length; index++)
            if (classLabels[index].contentEquals(classLabel)){
                classIndex = index;
                break;
            }
        return classIndex;
    }
    
    /**
     * Returns class label
     * @param classIndex class index
     * @return class label
     */
    public static String getClassLabel(byte classIndex){
        return classLabels[classIndex];
    }
    
    /**
     * Returns class labels
     * @return class labels
     */
    public static String[] getClassLabels (){
        return classLabels;
    }
    
    /**
     * Returns the number of examples of each class
     * @return number of examples of each class
     */
    public static long[] getClassNumExamples (){
        return classNumExamples;
    }
    
    /**
     * Returns the configuration object
     * @return Configuration object
     */
    public static Configuration getConfiguration (){
        return configuration;
    }
    
    /**
     * Returns database path
     * @return database path
     */
    public static String getDatabasePath (){
    	return outputPath+"/DB";
    }
    
    /**
     * Returns the maximum minutes of MapReduce task execution with no status update
     * @return maximum minutes of MapReduce task execution with no status update
     */
    public static int getHadoopMaxMinsNoUpdate (){
        return hadoopMaxMinsNoUpdate;
    }
    
    /**
     * Returns the number of Hadoop Mappers used for the execution
     * @return number of Hadoop Mappers used for the execution
     */
    public static int getHadoopNumMappers (){
        return hadoopNumMappers;
    }
    
    /**
     * Returns the number of Hadoop Reducers used for the execution
     * @return number of Hadoop Reducers used for the execution
     */
    public static int getHadoopNumReducers (){
        return hadoopNumReducers;
    }
    
    /**
     * Returns the HDFS location
     * @return HDFS location
     */
    public static String getHDFSLocation (){
        return hdfsLocation+"/";
    }
    
    /**
     * Returns input path
     * @return input path
     */
    public static String getInputPath (){
        return inputPath;
    }
    
    /**
     * Returns all input variables of the problem
     * @return all input variables of the problem
     */
    public static Variable[] getInputVariables(){
        return variables;
    }
    
    /**
     * Returns the number of classes
     * @return number of classes
     */
    public static byte getNumClasses (){
    	if (classLabels == null)
    		return 0;
    	if (classLabels.length<128)
    		return (byte)classLabels.length;
    	else{
    		System.err.println("\nTHE NUMBER OF CLASS LABELS ("+classLabels.length+") EXCEEDS THE LIMIT (127)\n");
    		System.exit(-1);
    		return -1;
    	}
    }
    
    /**
     * Returns the number of linguistic labels (fuzzy sets) considered for all variables
     * @return number of linguistic labels (fuzzy sets) considered for all variables
     */
    public static byte getNumLinguisticLabels (){
        return numLinguisticLabels;
    }
    
    /**
     * Returns the number of input variables
     * @return number of input variables
     */
    public static int getNumInputVariables (){
    	if (variables != null)
    		return variables.length;
    	else
    		return 0;
    }
    
    /**
     * Returns output path
     * @return output path
     */
    public static String getOutputPath (){
        return outputPath+"/";
    }
    
    /**
     * Returns the path where prototypes will be stored
     * @return path where prototypes will be stored
     */
    public static String getOutputPrototypesPath (){
        return outputPath+"/prototypes.txt";
    }
    
    /**
     * Returns the minimum number of examples required to generate a prototype
	 * @return the minimum number of examples required to generate a prototype
	 */
	public static int getPrototypeMinNumExamples() {
		return protoMinNumEx;
	}
    
    /**
     * Reads all the objects (variables, class labels, etc.) and parameters of this class
     * @throws Base64DecodingException 
     * @throws IOException 
     * @throws ClassNotFoundException 
     */
    public static void readConfiguration () throws Base64DecodingException, IOException, ClassNotFoundException{
    	
    	inputPath = configuration.get(INPUT_PATH_FIELD);
    	outputPath = configuration.get(OUTPUT_PATH_FIELD);
    	hdfsLocation = configuration.get(HDFS_LOCATION_FIELD);
    	
    	String aggStr = configuration.get(AGGREGATION_FIELD).toLowerCase().replaceAll("_", "").replaceAll("-", "");
    	if (aggStr.contentEquals("arithmeticmean") || aggStr.contentEquals("am"))
    		aggregation = AGGREGATION_ARITHMETIC_MEAN;
    	else if (aggStr.contentEquals("arithmeticmeansingleclass") || aggStr.contentEquals("amsingleclass"))
    		aggregation = AGGREGATION_ARITHMETIC_MEAN_SINGLE_CLASS;
    	else if (aggStr.contentEquals("none") || aggStr.contentEquals("no")
    			|| aggStr.contentEquals("not") || aggStr.contentEquals("false"))
    		aggregation = AGGREGATION_NONE;
    	else
    		throw new IOException("Aggregation not found");
    	protoMinNumEx = Integer.parseInt(configuration.get(PROTO_MIN_NUM_EX_FIELD));
    	numLinguisticLabels = Byte.parseByte(configuration.get(NUM_LINGUISTIC_LABELS_FIELD));
    	numClassLabels = Byte.parseByte(configuration.get(NUM_CLASSES_FIELD));
    	numVariables = Integer.parseInt(configuration.get(NUM_VARIABLES_FIELD));
    	
    	// Read class labels
    	byte[] bytes = Base64.decode(configuration.get(CLASS_LABELS_FIELD).getBytes());
    	ObjectInputStream objectInputStream = new ObjectInputStream(new ByteArrayInputStream(bytes));
    	classLabels = (String[])objectInputStream.readObject();
    	
    	// Read variables
    	bytes = Base64.decode(configuration.get(VARIABLES_FIELD).getBytes());
    	objectInputStream = new ObjectInputStream(new ByteArrayInputStream(bytes));
    	variables = (Variable[])objectInputStream.readObject();
    	
    	// Read the number of examples of each class
    	bytes = Base64.decode(configuration.get(CLASS_NUM_EXAMPLES_FIELD).getBytes());
    	objectInputStream = new ObjectInputStream(new ByteArrayInputStream(bytes));
    	classNumExamples = (long[])objectInputStream.readObject();
    		
    }
    
    /**
     * Reads the configuration of Hadoop
     * @throws Base64DecodingException 
     * @throws IOException 
     * @throws ClassNotFoundException 
     */
    public static void readHadoopConfiguration () throws Base64DecodingException, IOException, ClassNotFoundException{
    	
    	hadoopNumMappers = Integer.parseInt(configuration.get(HADOOP_NUM_MAPPERS_FIELD));
    	hadoopNumReducers = Integer.parseInt(configuration.get(HADOOP_NUM_REDUCERS_FIELD));
    	hadoopMaxMinsNoUpdate = Integer.parseInt(configuration.get(HADOOP_MAX_MINS_NO_UPDATE_FIELD));
    		
    }
    
    /**
     * Reads header file and generates fuzzy variables
     * @param filePath file path
	 * @throws IOException
     * @throws URISyntaxException 
     */
    public static void readHeaderFile (String filePath) throws IOException, URISyntaxException{
    	
    	Path pt=new Path(filePath);
        FileSystem fs = FileSystem.get(new java.net.URI(filePath),configuration);
        BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
    	String buffer = null;
        StringTokenizer st = null;
        
        String output = "";
        ArrayList<Variable> variablesTmp = new ArrayList<Variable>();
        ArrayList<Long> classNumExamples = new ArrayList<Long>();
        
        byte num_linguistic_labels = Byte.parseByte(configuration.get(NUM_LINGUISTIC_LABELS_FIELD));
        
        while ((buffer = br.readLine())!=null){
            
        	buffer = buffer.replaceAll(", ", ",");
        	st = new StringTokenizer (buffer);
        	String field = st.nextToken();
        	
        	// Attribute
        	if (field.contentEquals("@attribute")){
        		
        		// Attribute name
        		String attribute = st.nextToken();
        		String name = null, type = null;
        		
        		// Check format
        		if (!attribute.contains("{") && !attribute.contains("[")) {
        			name = attribute;
	    			while (st.hasMoreTokens() && !attribute.contains("{") && !attribute.contains("[")){
	    				type = attribute;
	    				attribute = st.nextToken();
	    			}
	        		if (!attribute.contains("{") && !attribute.contains("[")) {
	        			System.err.println("\nERROR READING HEADER FILE: Values are not specified\n");
	        			System.exit(-1);
	        		}
        		}
        		else if (attribute.contains("[")) {
        			System.err.println("\nERROR READING HEADER FILE: Invalid attribute name\n");
        			System.exit(-1);
        		}
        		else {
        			name = attribute.substring(0,attribute.indexOf("{"));
        			type = name;
        		}
        		
	    		// Nominal attribute
	    		if (type == name && attribute.contains("{")){
	
					// Get nominal values
	    			attribute = attribute.substring(attribute.indexOf("{")+1);
					st = new StringTokenizer (attribute,"{}, ");
					String[] nominalValues = new String[st.countTokens()];
					byte counter = 0;
					while (st.hasMoreTokens()){
	        			nominalValues[counter] = st.nextToken();
	        			counter++;
	        		}
					
					// Build a new nominal variable
					NominalVariable newVariable = new NominalVariable(name);
					newVariable.setNominalValues(nominalValues);
					
					variablesTmp.add(newVariable);
	    		
	    		}
	    		// Numeric attribute
	    		else if (attribute.contains("[")){
	    			
	    			// Check format
	    			if (type != name && !type.toLowerCase().contentEquals("integer") 
    					&& !type.toLowerCase().contentEquals("real")) {
		    				System.err.println("\nERROR READING HEADER FILE: Invalid attribute type: '"+type+"'\n");
	        				System.exit(-1);
	    			}
	    			else if (type == name && !attribute.toLowerCase().contains("integer") 
    					&& !attribute.toLowerCase().contains("real")){
		    				System.err.println("\nERROR READING HEADER FILE: No attribute type is specified\n");
	        				System.exit(-1);
	    			}
	    			
	    			// Get upper and lower limits
	    			st = new StringTokenizer (attribute.substring(attribute.indexOf("[")+1),"[], ");
	        			
	    			double lowerLimit = Double.parseDouble(st.nextToken());
	    			double upperLimit = Double.parseDouble(st.nextToken());
	    			
	    			// Integer attribute
	    			if (attribute.toLowerCase().contains("integer")){
	    				
	    				// If the number of integer values is less than the number of
	    				// linguistic labels, then build a nominal variable
	    				if ((upperLimit - lowerLimit + 1) <= num_linguistic_labels){
	    					String[] nominalValues = new String[(int)upperLimit-(int)lowerLimit+1];
	    					for (int i = 0; i < nominalValues.length; i++)
	    						nominalValues[i] = Integer.valueOf(((int)lowerLimit+i)).toString();
	    					NominalVariable newVariable = new NominalVariable(name);
	    					newVariable.setNominalValues(nominalValues);
	    					variablesTmp.add(newVariable);
	    				}
	    				else {
	    	    			FuzzyVariable newVariable = new FuzzyVariable(name);
	    	    			newVariable.buildFuzzySets(lowerLimit,upperLimit,num_linguistic_labels, FuzzyVariable.TYPE_INTEGER);
	    	    			variablesTmp.add(newVariable);
	    				}
	    				
	    			}
	    			// Real attribute
	    			else {
    	    			FuzzyVariable newVariable = new FuzzyVariable(name);
    	    			newVariable.buildFuzzySets(lowerLimit,upperLimit,num_linguistic_labels, FuzzyVariable.TYPE_REAL);
    	    			variablesTmp.add(newVariable);
    				}
	    			
	    		}
	    		else {
	    			System.err.println("\nERROR READING HEADER FILE: Invalid format\n");
        			System.exit(-1);
	    		}
        		
        	}
        	else if (field.contentEquals("@outputs")){
        		
        		st = new StringTokenizer (st.nextToken(),", ");
        		if (st.countTokens()>1){
        			System.err.println("\nERROR READING HEADER FILE: This algorithm does not support multiple outputs\n");
        			System.exit(-1);
        		}
        		output = st.nextToken();
        		
        	}
        	else if (field.contentEquals("@numInstancesByClass")){
        		
        		st = new StringTokenizer (st.nextToken(),", ");
        		while (st.hasMoreTokens())
        			classNumExamples.add(Long.parseLong(st.nextToken()));
        		
        	}
        	
        }
        
        if (classNumExamples.isEmpty()){
			System.err.println("\nERROR READING HEADER FILE: The number of examples of each class is not specified\n");
			System.exit(-1);
		}
        
        // Remove output attribute from variable list and save it as the class
        Iterator<Variable> iterator = variablesTmp.iterator();
        while (iterator.hasNext()){
        	Variable variable = iterator.next();
        	if (output.contentEquals(variable.getName())){
        		// Save class labels
        		saveClassLabels(((NominalVariable)variable).getNominalValues());
        		// Remove from the list
        		iterator.remove();
        		break;
        	}
        }
        Variable[] newVariables = new Variable[variablesTmp.size()];
        for (int i = 0; i < variablesTmp.size(); i++)
        	newVariables[i] = variablesTmp.get(i);
        
        // Save variables
        saveVariables(newVariables);
        
        // Save the number of examples of each class
        saveClassNumExamples(classNumExamples);
        
        configuration.set(NUM_CLASSES_FIELD, Byte.toString(numClassLabels));
        configuration.set(NUM_VARIABLES_FIELD, Integer.toString(numVariables));
    	
    }
    
    /**
     * Stores class labels in the configuration file
     * @param newClassLabels class labels to be stored
     * @throws IOException 
     */
    private static void saveClassLabels (String[] newClassLabels) throws IOException {
    	
    	ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
	    ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
	    objectOutputStream.writeObject(newClassLabels);
	    objectOutputStream.close();
	    String classLabelsStr = new String(Base64.encode(byteArrayOutputStream.toByteArray()));
	    configuration.set(CLASS_LABELS_FIELD,classLabelsStr);
    	classLabels = newClassLabels;
    	numClassLabels = (byte)newClassLabels.length;
    
    }
    
    /**
     * Stores the number of examples of each class in the configuration file
     * @param numExamplesByClass number of examples of each class
     * @throws IOException 
     */
    private static void saveClassNumExamples (ArrayList<Long> numExamplesByClass) throws IOException {
    	
    	// Convert into an array
    	long[] classNumExamplesArray = new long[numExamplesByClass.size()];
    	byte i = 0;
    	for (Long element:numExamplesByClass) {
    		classNumExamplesArray[i] = element.longValue();
    		i++;
    	}
    	
    	// Convert it into a string
    	ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
	    ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
	    objectOutputStream.writeObject(classNumExamplesArray);
	    objectOutputStream.close();
	    String outputString = new String(Base64.encode(byteArrayOutputStream.toByteArray()));
	    
	    configuration.set(CLASS_NUM_EXAMPLES_FIELD,outputString);
    	classNumExamples = classNumExamplesArray;
    	
    }
    
    /**
     * Stores the HDFS location in the configuration file
     * @param newHDFSLocation HDFS location (hdfs://remote_address:remote_port)
     */
    public static void saveHDFSLocation (String newHDFSLocation){
    	
    	configuration.set(HDFS_LOCATION_FIELD, newHDFSLocation);
    	hdfsLocation = newHDFSLocation;
    
    }
    
    /**
     * Stores input path in the configuration file
     * @param newInputPath input path
     */
    public static void saveInputPath (String newInputPath){
    	
    	configuration.set(INPUT_PATH_FIELD, newInputPath);
    	inputPath = newInputPath;
    
    }
    
    /**
     * Stores output path in the configuration file
     * @param newOutputPath output path
     */
    public static void saveOutputPath (String newOutputPath){
    	
    	configuration.set(OUTPUT_PATH_FIELD, newOutputPath);
    	outputPath = newOutputPath;
    
    }
    
    /**
     * Stores variables in the configuration file
     * @param newVariables variables to be added
     * @throws IOException 
     */
    private static void saveVariables (Variable[] newVariables) throws IOException {

    	ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
	    ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
	    objectOutputStream.writeObject(newVariables);
	    objectOutputStream.close();
	    String variablesStr = new String(Base64.encode(byteArrayOutputStream.toByteArray()));
	    configuration.set(VARIABLES_FIELD,variablesStr);
	    variables = newVariables;
    	numVariables = newVariables.length;
    	
    }
    
    /**
     * Sets a new configuration
     * @param newConfiguration configuration object
     */
    public static void setConfiguration (Configuration newConfiguration) {
    	configuration = newConfiguration;
    }
    
    /**
     * Stores the input configuration parameters in the configuration file
     * @param inputFilePath configuration file path
	 * @throws IOException 
     * @throws URISyntaxException 
     */
    public static void storeConfigurationParameters (String inputFilePath) throws IOException, URISyntaxException{
    	
    	Path pt = new Path(inputFilePath);
        FileSystem fs = FileSystem.get(new java.net.URI(inputFilePath),configuration);
        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
        String buffer = null;
        StringTokenizer st = null;
        
        while ((buffer = br.readLine())!=null){
        	if (!buffer.trim().isEmpty() && buffer.trim().charAt(0) != '*'){
	            st = new StringTokenizer (buffer, "= ");
	            if (st.countTokens() == 2)
	            	configuration.set(st.nextToken().toLowerCase(),st.nextToken().toLowerCase());
	            else
	            	throw new IOException();
        	}
        }
    	
    }

}
