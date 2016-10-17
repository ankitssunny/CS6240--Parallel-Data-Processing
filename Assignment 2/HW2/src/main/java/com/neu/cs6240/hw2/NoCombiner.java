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
public class NoCombiner {

	/* Mapper Class
	 * Intermediate Key and Value: Key is station ID and Value is a line consisting of recordType and recordValue
	 * if the recordType is either "TMAX" or "TMIN" else next line is read
	 */
	private static class readInput extends Mapper<Object, Text, Text, Text>{

		private Text recordValue = new Text();
		private Text stationID = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			// Reading the file line by line
			BufferedReader buf = new BufferedReader(new StringReader(value.toString()));
			String line = null;
			while( (line = buf.readLine()) != null) {
				String[] splitCsv = line.split(",");
				String recordType = splitCsv[2];
				if(recordType.equals("TMAX") || recordType.equals("TMIN")) {

					String temp = recordType + "," + splitCsv[3];
					stationID.set(splitCsv[0]);
					recordValue.set(temp);			
					context.write(stationID, recordValue);
				}
			}
			buf.close();
		}
	}

	/* Reducer Class
	 * Input key is the intermediate key. It reads all the values for a key and checks it for 
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
				String recordType = record[0];
				Float recordValue = Float.parseFloat(record[1]);
				
				if(recordType.equals("TMIN")) {
					sumMin += recordValue;
					countMin++;
				}	
				else {
					sumMax += recordValue;
					countMax++;
				}
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
		Job job = Job.getInstance(conf, "No Combiner");
		job.setJarByClass(NoCombiner.class);
		job.setMapperClass(readInput.class);
		job.setReducerClass(calculateMean.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}