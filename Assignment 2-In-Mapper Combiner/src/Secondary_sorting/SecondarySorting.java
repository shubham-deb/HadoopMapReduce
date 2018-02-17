package pckg4;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SecondarySorting {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf,"secondarysorting");
		
		job.setJarByClass(SecondarySorting.class);
		
		job.setMapperClass(StationYearTempMapping.class);
		job.setMapOutputKeyClass(StationYearKey.class);
		job.setMapOutputValueClass(Text.class);
		job.setGroupingComparatorClass(GroupingComparator.class);
		job.setReducerClass(StationReducer.class);
	
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(1);
        
		
        for(int i=1880;i<=1889;i++) {
        	String file = args[0]+"/"+i+".csv";
        	MultipleInputs.addInputPath(job, new Path(file),
					TextInputFormat.class,StationYearTempMapping.class);
        }
        
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		int flag = job.waitForCompletion(true)?0:1;
		
		FileSystem fs = FileSystem.get(new Configuration());
		
		if(flag==1)
			fs.delete(new Path(args[1]), true);
		
		System.exit(flag);
	}

}
