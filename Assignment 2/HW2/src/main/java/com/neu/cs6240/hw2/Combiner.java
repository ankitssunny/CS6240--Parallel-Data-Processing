package com.neu.cs6240.hw2;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/* This code is modified from the WordCount example provided on Hadoop website
 * https://hadoop.apache.org/docs/current/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html
 */
public class Combiner {

	// Counters to be used to calculate the number of times the Combiner is called.
	public static enum count{
		COMBINERCOUNT;
	}

	/* Mapper Class
	 * Intermediate Key and Value: Key is station ID and Value is a line 
	 * consisting of recordType and recordValue if the recordType is either "TMAX" or "TMIN"
	 * Each Output is like below:
	 * [Key:stationID], 
	 * [Value:"TMIN",recordValue if this line is TMIN else 0, 1 if this line is TMIN else 0,
	 * 		  "TMAX",recordValue if this line is TMAX else 0,1 if this line is TMAX else 0]
	 * 
	 */
	private static class readInput extends Mapper<Object, Text, Text, Text>{

		private Text recordValue = new Text();
		private Text stationID = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			// Reading the file line by line
			BufferedReader buf = new BufferedReader(new StringReader(value.toString()));
			String line = null;
			String tMin = null;
			String tMax = null;
			String one = "1";
			String zero = "0";
			
			while( (line = buf.readLine()) != null) {
				String[] splitCsv = line.split(",");
				String recordType = splitCsv[2];
				if(recordType.equals("TMAX") || recordType.equals("TMIN")) { 

					if(recordType.equals("TMAX")){ 
						tMin = "TMIN" + "," + zero + "," + zero; 
						tMax = "TMAX" + "," + splitCsv[3] + "," + one;
					}
					else if(recordType.equals("TMIN")) {
						tMin = "TMIN" + "," + splitCsv[3] + "," + one; 
						tMax = "TMAX" + "," + zero + "," + zero;
					}
					String temp = tMin + "," + tMax;
					stationID.set(splitCsv[0]);
					recordValue.set(temp);			
					context.write(stationID, recordValue);
				}
			}
			buf.close();
		}
	}

	/* 
	 * [Key:stationID], 
	 * [Value:"TMIN",recordValue if this line is TMIN else 0, 1 if this line is TMIN else 0,
	 * 		  "TMAX",recordValue if this line is TMAX else 0,1 if this line is TMAX else 0]
	 */ 
	private static class combineValues extends Reducer<Text,Text,Text,Text> {

		private Text result = new Text();

		public void reduce(Text key, Iterable<Text> values, Context context) 
				throws IOException, InterruptedException {

			// Increasing the global counter value to count how many times Combiner was called
			context.getCounter(count.COMBINERCOUNT).increment(1);

			// Variables used to compute the sum per station ID
			Float sumMin = 0f;
			Float countMin = 0f;
			Float sumMax = 0f;
			Float countMax = 0f;
			for (Text val : values) {
				// We know record[0] and record[3] are "TMIN" and "TMAX"
				String[] record = val.toString().split(",");
				String tMinValue = record[1];
				String tMinCount = record[2];
				String tMaxValue = record[4];
				String tMaxCount = record[5];

				sumMin += Float.parseFloat(tMinValue);
				countMin += Float.parseFloat(tMinCount);
				sumMax += Float.parseFloat(tMaxValue);
				countMax += Float.parseFloat(tMaxCount);
			}

			String tMin = "TMIN" + "," + sumMin.toString() + "," + countMin.toString();
			String tMax = "TMAX" + "," + sumMax.toString() + "," + countMax.toString();
			String tempResult = tMin + "," + tMax;
			result.set(tempResult);
			context.write(key, result);
		}
	}

	/* Reducer Class
	 * Input key is the intermediate key. It reads all the values for a key and checks if it for 
	 * recordType "TMAX" or "TMIN". Depending on this, it updates a local counter sumMax or sumMin respectively.
	 * It then writes the output in the format: station ID, MeanMinimum Temperature, MeanMaximum Temperature.
	 * If there are no values for either "TMAX" or "TMIN, it just prints "Null" in its place
	 */
	private static class calculateMean extends Reducer<Text,Text,Text,Text> {

		private Text result = new Text();

		public void reduce(Text key, Iterable<Text> values, Context context) 
				throws IOException, InterruptedException {

			// Variables used to compute the Average minimum and average maximum per station ID
			String temp1, temp2;
			Float sumMin = 0f;
			Float countMin = 0f;
			Float sumMax = 0f;
			Float countMax = 0f;
			Float averageMin = 0f; 
			Float averageMax = 0f;		

			for (Text val : values) {

				String[] record = val.toString().split(",");
				String tMinValue = record[1];
				String tMinCount = record[2];
				String tMaxValue = record[4];
				String tMaxCount = record[5];

				sumMin += Float.parseFloat(tMinValue);
				countMin += Float.parseFloat(tMinCount);
				sumMax += Float.parseFloat(tMaxValue);
				countMax += Float.parseFloat(tMaxCount);
			}
			// Computing the average minimum temperature and average maximum temperature
			averageMin = (countMin == 0f) ? 0f : (sumMin/countMin);
			averageMax = (countMax == 0f) ? 0f : (sumMax/countMax);
			temp1 = (averageMin == 0f) ? "Null" : averageMin.toString();
			temp2 = (averageMax == 0f) ? "Null" : averageMax.toString();
			String tempResult = temp1 + "," + temp2;
			result.set(tempResult);
			context.write(key, result);
		}
	}

	// Calling Method
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Combiner");
		job.setJarByClass(Combiner.class);
		job.setMapperClass(readInput.class);
		job.setCombinerClass(combineValues.class);
		job.setReducerClass(calculateMean.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}