package pckg3;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class InMapperCombiner {

	public static class MappingStation extends Mapper<Object, Text, Text, Text>{
		
		// here we set a heap size as 50000
		private static final int FLUSH_LIMIT = 50000;
		LinkedHashMap<String,LinkedHashMap<String,ArrayList<Integer>>> hmap;
		private Text stationId = new Text();
		
		// create a hashmap whenever mapper is called
		protected void setup(Context context)
	              throws IOException,InterruptedException{
			hmap = new LinkedHashMap<String,LinkedHashMap<String,ArrayList<Integer>>>();
		}
		
		public void map(Object key,Text values,Context context) 
				throws IOException, InterruptedException {
			
			String[] stationInfo = values.toString().split(",");
			
			String stationNum = stationInfo[0];
			int temp = Integer.parseInt(stationInfo[3]);
			String tempType = stationInfo[2];
			
			if(tempType.equals("TMAX") || tempType.equals("TMIN")) {
				
				stationId.set(stationNum);
				
				// if there is a previous entry of stationId in the HashMap,
				// then we look at the inner HashMap which has "TMAX" AND "TMIN"
				// as the key and the sum and count as it's value
				if(hmap.get(stationNum)!=null) {				
					LinkedHashMap<String,ArrayList<Integer>> map = hmap.get(stationNum);
					ArrayList<Integer> newSumCountVals = new ArrayList<Integer>();
					
					// if the innerHashMap does have the instance, then we 
					// update the sum and count values for the instance of 
					// that class
					if(map.get(tempType)!=null) {
						ArrayList<Integer> sumCountVals = map.get(tempType);
						newSumCountVals.add(sumCountVals.get(0)+temp);
						newSumCountVals.add(sumCountVals.get(1)+1);	
						map.put(tempType,newSumCountVals);
					}
					// if the innerHashMap doesn't have an instance of "TMAX" or
					// "TMIN", then we add the new instance to the stationId hashmap
					else {
						ArrayList<Integer> sumCountVals = new ArrayList<Integer>();
						sumCountVals.add(0,temp);
						sumCountVals.add(1,1);	
						map.put(tempType,sumCountVals);
					}
					// we put the new value into the hashmap
					hmap.put(stationNum, map);
				}else {		
					// we put the new value into the hashmap
					LinkedHashMap<String,ArrayList<Integer>> map = new LinkedHashMap<String,ArrayList<Integer>>();
					ArrayList<Integer> sumCountVals = new ArrayList<Integer>();
					sumCountVals.add(temp);
					sumCountVals.add(1);
					map.put(tempType,sumCountVals);
					hmap.put(stationNum, map);
				}
			}
			
			// we check after insertion whether the hashmap is out of heap size,
			// if it is then we get all the data from the hashmap and send it to
			// the reducer and clear the hashmap
			if(check_limit(context,false)){
				
				for(String k:hmap.keySet()) {
					LinkedHashMap<String,ArrayList<Integer>> map = hmap.get(k);
					for(String str:map.keySet()) {
						ArrayList<Integer> ar = map.get(str);
						context.write( new Text(k), new Text(str+","+ar.get(0)+
								","+ar.get(1)));							
						}
				}
				hmap.clear();
			}
							
		}
		
		// checks if the hash map is out of memory
		private boolean check_limit(Context context, boolean force) 
				throws IOException, InterruptedException {
			if(!force) {
				int size = hmap.size();
				if(size<FLUSH_LIMIT) {
					return false;
				}
			}
			return true;
		}
		
		// this function gets called before the map execution ends
		// and clears the hashmap
		protected void cleanup(Context context)
                throws IOException,InterruptedException{
			// force flush 
			for(String k:hmap.keySet()) {
				LinkedHashMap<String,ArrayList<Integer>> map = hmap.get(k);
				for(String str:map.keySet()) {
					ArrayList<Integer> ar = map.get(str);
					context.write( new Text(k), new Text(str+","+ar.get(0)+
										","+ar.get(1)));		
					}
			}
			hmap.clear();
		}

	}

	public static class TempReducer extends Reducer <Text,Text,Text,Text>{
		
		public void reduce(Text key, Iterable<Text> values
							,Context context) 
				throws IOException, InterruptedException {
			
			Text result = new Text();
			int minSum=0,maxSum=0,minCount=0,maxCount=0;

			for(Text val:values) {
								
				String[] arr = val.toString().split(",");
				int temp = Integer.parseInt(arr[1]);
				int count = Integer.parseInt(arr[2]);
				
				if(arr[0].equals("TMAX")) {
					maxSum+=temp;
					maxCount+=count;
				}else if(arr[0].equals("TMIN")) {
					minSum+=temp;
					minCount+=count;
				}
				
			}				
				float meanMin = 0,meanMax=0;
				
				if(minCount!=0) {
					meanMin = (float)minSum/minCount ;
				}
				else {
					meanMin = 0;
				}
				
				if(maxCount!=0) {
					meanMax = (float)maxSum/maxCount;
				}
				else {
					meanMax = 0;
				}
			
				String value = ""+meanMax+","+meanMin;
				result.set(value);
				
				// we output the meanMax and meanMin with the stationid as the result
				context.write(key, result);			
		}
	}
	
	//driver class
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf,"inmappercombiner");
		
		job.setJarByClass(InMapperCombiner.class);
		
		job.setMapperClass(MappingStation.class);
		job.setReducerClass(TempReducer.class);
		job.setNumReduceTasks(1);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		int flag = job.waitForCompletion(true)?0:1;
		
		FileSystem fs = FileSystem.get(new Configuration());
		
		// if the job is not complete then delete the output folder
		if(flag==1)
			fs.delete(new Path(args[1]), true);
		
		System.exit(flag);

	}

}