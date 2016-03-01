package snedeker.cc.project2;

import java.io.IOException;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedList;
import java.util.Locale;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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
	
	final static private int WINDOW_SIZE = 3;

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
			
//			// Create a simple date formatter for reading the date
//			DateFormat format = new SimpleDateFormat("yyyy-MM-dd", Locale.ENGLISH);
//			format.setLenient(false);
//			Date date = null;
//			try {
//				date = format.parse(values[1]);
//			} catch (ParseException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}
//			// Convert the Java Date into a long value
//			long longDate = date.getTime();
//			
//			// Set the values in the Time_Series
//			series.set(longDate, Double.parseDouble(values[2]));
//			// Set the word to be the company code
//			word.set(values[0]);
//			// Write the values back out to the mapper context
//			context.write(word, series);
		}
	}
	
	/**
	 * This is the Reducer component.  It will take the Mapped, Shuffled, and Sorted data,
	 * and output the means.
	 * 
	 * @author Colby Snedeker
	 *
	 */
	public static class Reduce extends Reducer<Text, Text, Text, Text> {

		/**
		 * This is the reduce function.  It iterates through all of the Time_Series values to 
		 * compute the 3 and 4 window means.  It then takes those means and outputs a key 
		 * value pair to the Reducer context that consists of the company code as the key, 
		 * and a string providing the means in a formatted way as the value.
		 */
		public void reduce(Text key, Text values, Context context) throws IOException, InterruptedException {
			
//			Configuration conf = context.getConfiguration();
//			int windowSize = Integer.parseInt(conf.get("window.size"));
//			
//			int seriesCount = 0;
//			LinkedList<Double> prices = new LinkedList<>();
//			String companyCode = key.toString();
//			
//			// Sort the Time_Series by date
//			TreeMap<Long, Double> sortedSeries = new TreeMap<>();
//			for (Time_Series series : values) {
//				sortedSeries.put(series.getTimestamp(), series.getValue());
//			}
//			
//			// Iterate through the series for the company, and compute both the 3 and 4 window means
//			for (java.util.Map.Entry<Long, Double> entry : sortedSeries.entrySet()) {
//				Text means = new Text();
//				double total = 0;
//				
//				seriesCount++;
//				
//				if (prices.size() >= windowSize)
//					prices.remove();
//				
//				prices.add(entry.getValue());
//				
//				double mean = 0;
//				
//				if (seriesCount < windowSize) {
//					for (int i = 0; i < seriesCount; i++) {
//						total += prices.get(i);
//					}
//					
//					mean = total / (double) seriesCount;
//				}
//				else {
//					for (int i = 0; i < windowSize; i++) {
//						total += prices.get(i);
//					}
//					
//					mean = total / (double) windowSize;
//				}
//				
//				DecimalFormat df = new DecimalFormat("#.00");
//
//				// Write the output string to the means variable
//				means.set(df.format(mean));
//				
//				SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd");
//				String date = dateFormatter.format(new Date(entry.getKey()));
//				
//				// Properly set the key
//				key.set(companyCode + "," + date);
//					
//				// Write the company code and means to the reducer context
//				context.write(key, means);
//			}
			
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
		conf.set("window.size", args[0]);
		conf.set("mapred.textoutputformat.separator", ",");
		Job job = new Job(conf, "runningMean");
		job.setJarByClass(snedeker.cc.project2.MatrixMultiplication.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
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
		FileInputFormat.addInputPath(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		//Launch the job and wait for it to finish
//		job.waitForCompletion(true);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		
	}
	
}
