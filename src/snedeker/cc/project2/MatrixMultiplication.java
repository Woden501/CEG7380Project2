package snedeker.cc.project2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class MatrixMultiplication {
	
	/**
	 * This is the Mapper component.  It will take the input data and separate it into
	 * individual entries which will later be combined and averaged.
	 * 
	 * @author Colby Snedeker
	 *
	 */
	public static class Map extends Mapper<LongWritable, Text, Text, Text> {
		// Create the variables that will hold the (i,j) key, and (matrix, j, number) value
		private Text resultPosition = new Text();
		private Text multiplicationValue = new Text();
		
		/**
		 * This is the map function.  In this function the lines are read and tokenized.  The 
		 * date and price information are placed into a Time_Series object.  This object is then
		 * placed into the Mapper context as the value and the company code as the key.
		 */
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// Read in the first line
			String line = value.toString();
			
			// Get the result matrix size information from the configuration
			Configuration conf = context.getConfiguration();
			int resultMatrixI = Integer.parseInt(conf.get("result.i"));
			int resultMatrixK = Integer.parseInt(conf.get("result.k"));
			
			// Split the line on the "," delimiter
			// The resultant String array values are
			// value[0] - matrix, value[1] - row index, value[2] - column inex, value[3] - value
			String[] values = line.split(",");
			
			if (values[0].equalsIgnoreCase("A")) {
				for (int k = 0; k < resultMatrixK; k++) {
					resultPosition.set(values[1] + "," + k);
					multiplicationValue.set(values[0] + "," + values[2] + "," + values[3]);
					
					context.write(resultPosition, multiplicationValue);
				}
			}
			else {
				for (int i = 0; i < resultMatrixI; i++) {
					resultPosition.set(i + "," + values[2]);
					multiplicationValue.set(values[0] + "," + values[1] + "," + values[3]);
					
					context.write(resultPosition, multiplicationValue);
				}
			}
		}
	}
	
	/**
	 * This is the Reducer component.  It will take the Mapped, Shuffled, and Sorted data,
	 * and output the means.
	 * 
	 * @author Colby Snedeker
	 *
	 */
	public static class Reduce extends Reducer<Text, Text, Text, DoubleWritable> {

		/**
		 * This is the reduce function.  It iterates through all of the Time_Series values to 
		 * compute the 3 and 4 window means.  It then takes those means and outputs a key 
		 * value pair to the Reducer context that consists of the company code as the key, 
		 * and a string providing the means in a formatted way as the value.
		 */
		public void reduce(Text key, Iterable<Text> multiplicationValues, Context context) throws IOException, InterruptedException {
			
			TreeMap<Integer, ArrayList<Double>> jOrderedValues = new TreeMap<>();
			double sum = 0;
			
			// Read the multiplicationValues and insert the j position and value into a key value pair {j position, value}.
			// In this case we don't care what matrix the value is from as we are just going to be multiplying all values
			// with the same j key together later.
			for (Text text : multiplicationValues) {
				String multiplicationValue = text.toString();
				String[] values = multiplicationValue.split(",");
				
//				jOrderedValues.put(Integer.parseInt(values[1]), Double.parseDouble(values[2]));
				
				ArrayList<Double> positionValues = jOrderedValues.get(Integer.parseInt(values[1]));
				
				if (positionValues != null) {
					positionValues.add(Double.parseDouble(values[2]));
				}
				else {
					positionValues = new ArrayList<>();
					positionValues.add(Double.parseDouble(values[2]));
					jOrderedValues.put(Integer.parseInt(values[1]), positionValues);
				}
			}
			
			for (Entry<Integer, ArrayList<Double>> entry : jOrderedValues.entrySet()) {
				ArrayList<Double> values = entry.getValue();
				sum += values.get(0) * values.get(1);
			}
			
			context.write(key, new DoubleWritable(sum));
		}
	}
	
	/**
	 * Configures the Hadoop job, and reads the user provided arguments
	 * 
	 * @param args The user provided arguments.
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		//Get configuration object and set a job name
		Configuration conf = new Configuration();
		conf.set("result.i", args[0]);
		conf.set("result.k", args[1]);
		conf.set("mapred.textoutputformat.separator", ",");
		Job job = new Job(conf, "matrixMultiplication");
		job.setJarByClass(snedeker.cc.project2.MatrixMultiplication.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		//Set key, output classes for the job (same as output classes for Reducer)
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		
		//Set format of input files; "TextInputFormat" views files
		//as a sequence of lines
		job.setInputFormatClass(TextInputFormat.class);
		//Set format of output files: lines of text
		job.setOutputFormatClass(TextOutputFormat.class);
		//job.setNumReduceTasks(2); #set num of reducers
		//accept the hdfs input and output directory at run time
		FileInputFormat.addInputPath(job, new Path(args[2]));
		FileOutputFormat.setOutputPath(job, new Path(args[3]));
		//Launch the job and wait for it to finish
//		job.waitForCompletion(true);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		
	}
	
}
