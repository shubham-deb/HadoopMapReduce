package hw2;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

public class pagerank {
	
	// GLOBAL COUNTERS
	static enum UpdateCount {
		nodes, dangling_nodes, dangling_nodes_sum, total_pr_sum, final_dangling_node_sum,final_pr_sum
	}
	static double dangling_sum = 0,pagerank_sum=0;
//		
	public static void main(String[] args) throws InterruptedException, Exception {
			Configuration conf = new Configuration();
			long startTime,endTime;
			// we keep track of the execution time of the job with startTime and endTime
			startTime = System.currentTimeMillis();
			// execution parsing job
			String dir = args[0].substring(0, args[0].lastIndexOf('/'));
			Job parsingJob = parsingJob(args,dir, conf);
			endTime = System.currentTimeMillis();
			System.out.println("The total time taken by the parser is "+(endTime-startTime));
			conf.setLong("numnodes", parsingJob.getCounters().findCounter(UpdateCount.nodes).getValue());
			conf.setDouble("alpha", 0.15);
			
			System.out.println("-------------------------------------------------------------");
			System.out.println("Starting PAGERANK JOB");
			
			startTime = System.currentTimeMillis();
			Counter danglingNodesPRSum;
			int count=0;
			for(int itr=0;itr<10;itr++) {
				count++;
				conf.setInt("iteration", itr);
				// starting pagerank job
				String direc = args[0].substring(0, args[0].lastIndexOf('/'));
				Job pagerankJob = pagerankingJob(direc,itr,conf);
				// keep track of the previous dangling node sum
	            		danglingNodesPRSum = pagerankJob.getCounters().findCounter(UpdateCount.dangling_nodes_sum);
				// set it in the global counter to be used in the next iteration
				conf.setDouble("dangling_nodes_sum",(double)danglingNodesPRSum.getValue()/Math.pow(10, 9));
			}
			endTime = System.currentTimeMillis();
			System.out.println("Total time taken by the pagerank is "+(endTime-startTime));
			
			System.out.println("-------------------------------------------------------------");
			System.out.println("Starting TOP 100 JOB");

			startTime = System.currentTimeMillis();
			@SuppressWarnings("unused")
			// top100 job
			Job top100Job = top100Job(dir+"/processed_data"+count,args[args.length-1],conf);
			endTime = System.currentTimeMillis();
			System.out.println("Total time taken by the top100 is "+(endTime-startTime));
	}
	
	public static Job parsingJob(String[] inputPath,String directory, Configuration conf)
			throws IOException, Exception, InterruptedException {
		Job job = Job.getInstance(conf, "job1");
		job.setJarByClass(pagerank.class);

		job.setMapperClass(Job1Mapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(GraphNode.class);

		job.setReducerClass(Job1Reducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(GraphNode.class);
		job.setNumReduceTasks(1);

		//SequenceFileOutputFormat helps to write into the file in the form of key and value types
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		for(int i=0;i<inputPath.length-1;i++)
			FileInputFormat.addInputPath(job, new Path(inputPath[i]));
		FileOutputFormat.setOutputPath(job, new Path(directory+"/processed_data0"));

		job.waitForCompletion(true);
		return job;
	}

	public static Job pagerankingJob(String inputPath,int iteration, Configuration conf)
			throws IOException, Exception, InterruptedException {
		Job job = Job.getInstance(conf, "job2");
		job.setJarByClass(pagerank.class);

		job.setMapperClass(Job2Mapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(GraphNode.class);

		job.setReducerClass(Job2Reducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(GraphNode.class);
        	job.setInputFormatClass(SequenceFileInputFormat.class);	
        	job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setNumReduceTasks(1);

		FileInputFormat.addInputPath(job, new Path(inputPath+"/processed_data"+iteration));
		FileOutputFormat.setOutputPath(job, new Path(inputPath+"/processed_data"+(iteration+1)));

		job.waitForCompletion(true);
		return job;
	}
	
	public static Job top100Job(String inputPath,String outputPath, Configuration conf) 
			throws IOException, ClassNotFoundException, InterruptedException {
		Job job = Job.getInstance(conf, "job3");
		job.setJarByClass(pagerank.class);
		
        	job.setInputFormatClass(SequenceFileInputFormat.class);	
		job.setMapperClass(Job3Mapper.class);
		job.setMapOutputKeyClass(DoubleWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setSortComparatorClass(KeyComparator.class);
		job.setReducerClass(Job3Reducer.class);
		job.setOutputKeyClass(DoubleWritable.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(1);
        
		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		job.waitForCompletion(true);
		return job;
	}

}
