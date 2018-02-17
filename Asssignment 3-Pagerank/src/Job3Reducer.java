package hw2;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Job3Reducer extends Reducer<DoubleWritable, Text, Text, DoubleWritable>{
	static int count = 1;
	public void reduce(DoubleWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		
		// to keep track of the first 100 pages
		if(count<=100) {
			Text pagename = new Text();
			for(Text val:values) {
				pagename.set(val);
				break;
			}
			// we output the pagename and it's current pagerank
			context.write(new Text(count+". "+pagename), key);
		}
		count+=1;
	}
}
