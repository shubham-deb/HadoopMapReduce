package hw2;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class Job2Mapper extends Mapper<Text, GraphNode, Text, GraphNode>{
	int iteration;
	double pagecount;
	
	public void setup(Context context) {
		Configuration conf = context.getConfiguration();
		iteration = conf.getInt("iteration", -1);
		pagecount = conf.getDouble("numnodes", (double)-1);
		
		if(iteration == -1 || pagecount == -1)
			throw new Error("Invlaid iteration / pagecount");
	}
	
	
	public void map(Text key, GraphNode value, Context context) 
			throws IOException, InterruptedException {
		
		GraphNode node = new GraphNode();
		// if it is the current iteration, then we set the pagerank 
		// as 1/pagecount initially
		if(iteration == 0) {
			value.setPagerank((double)1/pagecount);
		}
		
		int neighbornodes = value.getOutlinks().size();
		
		// send the contribution of this node's pagerank equally to all
		// it's neighbors 
		for(String neighborpagenames:value.getOutlinks()) {
			// we send this page's pagerank contribution to the current neigboring node
			node.setContributorsPRSum((double)value.getPagerank()/neighbornodes);
			node.setDangling(true);
			context.write(new Text(neighborpagenames), node);
		}
		
		// we also emit the current pagename and it’s adjacency list to 
		// the reducer
		context.write(key, value);
	}
}
