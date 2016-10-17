package com.neu.cs6240.hw2;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/* This code is modified from the MaxTemperature example from the link
 * https://nuonline.neu.edu/courses/1/CS6240.13036.201710/content/_9387330_1/story_content/external_files/MaxTemperatureUsingSecondarySort.java 
 * I have used In Map Combining approach to reduce the amount of data processing on the Reducer
 */
public class SecSort {

	/*
	 * Mapper which uses in-map combining to reduce computation at Reducer.
	 * Record: [Key: (stationID,year) Value:(TMIN,tminval,tmincount,TMIN,tmaxvalue,tmaxcount,year)] 
	 */
	static class PerStationByYearMapper extends Mapper<Object, Text, Text, Text> {

		private Map<String, String> aggregateRecord;
		private Text stationID = new Text();
		private Text record = new Text();

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			aggregateRecord = new HashMap<String, String>();
		}

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			BufferedReader buf = new BufferedReader(new StringReader(value.toString()));
			// Variables used to create the out value for the mapper
			String line = null;
			String tMin = null;
			String tMax = null;
			String one = "1";
			String zero = "0";
			String temp = null;

			// Reading the file line by line
			while( (line = buf.readLine()) != null) {
				String[] splitCsv = line.split(",");
				String recordType = splitCsv[2];
				String ID = splitCsv[0];
				String year= splitCsv[1].substring(0, 4);
				String intermediateKey = ID + "," + year;	// The key is composed of stationID and year in Map

				if(recordType.equals("TMAX") || recordType.equals("TMIN")) { 

					/* Now checking if the stationID exists or not. If not then create the stationID
					 *  with the key and value else update the existing value for the stationID 
					 */
					if(aggregateRecord.containsKey(intermediateKey)) {
						// Record: [Key: (stationID,year) Value:(tmin,tminval,tmincount,tmax,tmaxvalue,tmaxcount,year)] 
						String[] record = aggregateRecord.get(intermediateKey).split(",");
						Float TMINValue = Float.parseFloat(record[1]);
						Float TMINCount = Float.parseFloat(record[2]);
						Float TMAXValue = Float.parseFloat(record[4]);
						Float TMAXCount = Float.parseFloat(record[5]);
						String currYear = record[6];
						Float tempVal 	= Float.parseFloat(splitCsv[3]);

						// Only updating the values which this record corresponds to i.e. either TMIN or TMAX
						if(recordType.equals("TMIN")) {
							TMINValue += tempVal;
							TMINCount ++;
						}

						else {
							TMAXValue += tempVal;
							TMAXCount++;
						}

						temp = "TMIN" + "," + TMINValue.toString() + "," + TMINCount.toString() + "," 
								+ "TMAX" + "," + TMAXValue.toString() + "," + TMAXCount.toString() + "," + currYear;

						// Writing the updated value to Map
						aggregateRecord.put(intermediateKey,temp);
					}

					else {
						if(recordType.equals("TMIN")){ 
							tMin = "TMIN" + "," + splitCsv[3] + "," + one; 
							tMax = "TMAX" + "," + zero + "," + zero;
						}

						else {
							tMin = "TMIN" + "," + zero + "," + zero; 
							tMax = "TMAX" + "," + splitCsv[3] + "," + one;
						}
						// writing new values to Map
						temp = tMin + "," + tMax + "," + year ;
						aggregateRecord.put(intermediateKey,temp);
					}
				}
			}
			buf.close();
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {

			// For all the station ID's, sending their cumulative temperature values
			for (String ID : aggregateRecord.keySet()) {
				stationID.set(ID);
				record.set(aggregateRecord.get(ID));
				context.write(stationID, record);
			}
			// clearing out the Map
			aggregateRecord.clear();
		}
	}

	/*
	 * Reducer
	 * Record: [Key: (stationID,year) Value:(TMIN,tminval,tmincount,TMIN,tmaxvalue,tmaxcount,year)] 
	 * Right now working on the assumption that only one stationID is given to each station.
	 */
	static class PerStationByYearReducer extends Reducer<Text, Text, Text ,Text> {
		
		Text key = new Text();									// Stores the final key i.e. stationID	
		private Text result = new Text();						// Stores the final result i.e. a list of (year,Min,Max) values
		private Map<Integer, String> yearMap;					// used to compute temperatures year wise
		public SortedSet<String> value = new TreeSet<String>(); // stores the final list of values for a year
		
		// Initializing the map before any reduce functions.
		@Override
		protected void setup(Context context) {
			yearMap  = new HashMap<Integer, String>();
		}
		
		/*The reduce method takes in a (stationID,year) as key and all its related values. It then computes the total
		 * sum and count of minimum and maximum temperatures and writes it to the yearMap.
		 * The Reducer will receive the keys sorted by station ID. Each reduce function will work on exactly one
		 * station ID's value. The values wont be sorted in any order. 
		 */
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {    

			String stationID = key.toString().split(",")[0];
			Float tMinValue;
			Float tMinCount;
			Float tMaxValue;
			Float tMaxCount;
			
			for (Text val : values) {

				String[] record = val.toString().split(",");
				tMinValue = Float.parseFloat(record[1]);
				tMinCount = Float.parseFloat(record[2]);
				tMaxValue = Float.parseFloat(record[4]);
				tMaxCount = Float.parseFloat(record[5]);
				Integer year = Integer.parseInt(record[6]);

				// Checking if the record for this year exists in this reduce function. If yes, then the record is updated
				// otherwise a new record is created for this year and this stationID

				if(yearMap.containsKey(year)) {

					String[] oldRecord = yearMap.get(year).split(",");
					Float oldTMinValue = Float.parseFloat(oldRecord[1]);
					Float oldTMinCount = Float.parseFloat(oldRecord[2]);
					Float oldTMaxValue = Float.parseFloat(oldRecord[4]);
					Float oldTMaxCount = Float.parseFloat(oldRecord[5]);

					tMinValue += oldTMinValue;
					tMinCount += oldTMinCount;
					tMaxValue += oldTMaxValue;
					tMaxCount += oldTMaxCount;

					String temp = "TMIN" + "," + tMinValue.toString() + "," + tMinCount.toString() + "," 
								+ "TMAX" + "," + tMaxValue.toString() + "," + tMaxCount.toString();
					yearMap.put(year, temp);
				}
				else {
					String temp = "TMIN" + "," + tMinValue.toString() + "," + tMinCount.toString() + "," 
								+ "TMAX" + "," + tMaxValue.toString() + "," + tMaxCount.toString();
					yearMap.put(year, temp);
				}
			}					

			/* For all the years in the map for this stationID, computing the MeanMinimum and MeanMaximum values  
			 * and writing it to a SortedSet so that it is stored in sorted order for each stationID
			 */
			for(Integer yr: yearMap.keySet()) {
			
				String[] record = yearMap.get(yr).split(",");
				Float averageMin = 0f; 
				Float averageMax = 0f;	
				tMinValue = Float.parseFloat(record[1]);
				tMinCount = Float.parseFloat(record[2]);
				tMaxValue = Float.parseFloat(record[4]);
				tMaxCount = Float.parseFloat(record[5]);
				
				averageMin = (tMinCount == 0f) ? 0f : (tMinValue/tMinCount);
				averageMax = (tMaxCount == 0f) ? 0f : (tMaxValue/tMaxCount);
				String temp1 = (averageMin == 0f) ? "Null" : averageMin.toString();
				String temp2 = (averageMax == 0f) ? "Null" : averageMax.toString();
				String tempResult = "(" + yr.toString() + "," + temp1 + "," + temp2 + ")";

				value.add(tempResult);
			}
			
			key.set(stationID);
			result.set(value.toString());
			context.write(key, result);
			
			// Clearing out yearMap, value and perYearValue to be used by next stationID
			yearMap.clear();
			value.clear();	
		}
	}

	// The partition should be on the basis of only stationID and not the year
	public static class OnStationIDPartitioner extends Partitioner<Text, Text> {

		@Override
		public int getPartition(Text key, Text value, int numPartitions) {

			String stationID = key.toString().split(",")[0];
			// multiply the hashCode of the stationID by 127 to perform some mixing
			return Math.abs( stationID.hashCode() * 127) % numPartitions;
		}
	}


	/* The keyComparator should first sort on the basis of the stationID and then sort on the basis of the 
	 * year for the stationID. We dont need it in Secondary Sort example. But just keeping it here 
	 * because I wanna tinker with it later.
	 */
	public static class KeyComparator extends WritableComparator {

		protected KeyComparator() {
			super(Text.class, true);
		}

		@SuppressWarnings("rawtypes")
		@Override
		public int compare(WritableComparable w1, WritableComparable w2) {

			Text id1 = (Text) w1;
			Text id2 = (Text) w2;

			String stationID1 = id1.toString().split(",")[0];
			Integer year1 = Integer.parseInt(id1.toString().split(",")[1]);
			String stationID2 = id2.toString().split(",")[0];
			Integer year2 = Integer.parseInt(id2.toString().split(",")[1]);

			int cmp = stationID1.compareTo(stationID2);
			if (cmp != 0) {
				return cmp;
			}
			return year1.compareTo(year2);
		}
	}

	/* The Group comparator groups the keys on the basis on only StationID which means all records
	 * with same stationID will end up at the same Reducer.
	 */
	public static class GroupComparator extends WritableComparator {

		protected GroupComparator() {
			super(Text.class, true);
		}

		@SuppressWarnings("rawtypes")
		@Override
		public int compare(WritableComparable w1, WritableComparable w2) {

			String stationID1 = ((Text) w1).toString().split(",")[0];
			String stationID2 = ((Text) w2).toString().split(",")[0];
			return stationID1.compareTo(stationID2);
		}
	}

	// Calling Method
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Secondary Sort");
		job.setJarByClass(SecSort.class);
		job.setPartitionerClass(OnStationIDPartitioner.class);
		//job.setSortComparatorClass(KeyComparator.class);
		job.setGroupingComparatorClass(GroupComparator.class);
		job.setMapperClass(PerStationByYearMapper.class);
		job.setReducerClass(PerStationByYearReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}