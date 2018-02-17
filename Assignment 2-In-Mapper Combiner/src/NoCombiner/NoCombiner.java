package pckg1;


import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class NoCombiner {
	
	public static class MappingStationId extends Mapper<Object, Text, Text, Text>{
		
		public void map(Object key,Text values,Context context) 
				throws IOException, InterruptedException{
			
			Text stationId = new Text();
			Text station = new Text();
			
			String[] stationInfo = values.toString().split(",");
			
			if(stationInfo[2].equals("TMAX") || stationInfo[2].equals("TMIN")) {
			
				stationId.set(stationInfo[0]);
				
				String tempInfo = stationInfo[2] +","+ stationInfo[3];
				station.set(tempInfo);
				
				// we emit the stationId as the key and "TMAX or TMIN" with the 
				// temperature as the value
				context.write(stationId,station);
			}
		}
	}
	
	public static class IntTempReducer extends Reducer<Text,Text,Text,Text>{
		
		public void reduce(Text key, Iterable<Text> values,Context context ) 
				throws IOException, InterruptedException {
			
			Text result = new Text();
			int minSum=0,maxSum=0,minCount=0,maxCount=0;
			
			for(Text val:values) {
				String[] arr = val.toString().split(",");
				int temp = Integer.parseInt(arr[1]);
				
				if(arr[0].equals("TMAX")) {
					maxSum+=temp;
					maxCount++;
				}else if(arr[0].equals("TMIN")) {
					minSum+=temp;
					minCount++;
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
		Job job = Job.getInstance(conf,"nocombiner");
		
		job.setJarByClass(NoCombiner.class);
		
		job.setMapperClass(MappingStationId.class);
		job.setReducerClass(IntTempReducer.class);
		job.setNumReduceTasks(1);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		System.exit(job.waitForCompletion(true)?0:1);
	}

}
