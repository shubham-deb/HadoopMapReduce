package hw2;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class Job3Mapper extends Mapper<Text, GraphNode, DoubleWritable, Text>{

	public void map(Text key, GraphNode value, Context context) 
			throws IOException, InterruptedException {
		// Here since we are comparing pageranks, we keep the value of the 
		// pagerank as the key and the pagename as the value. 
		context.write(new DoubleWritable(value.getPagerank()),key);
	}
}
