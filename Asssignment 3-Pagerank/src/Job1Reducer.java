package hw2;

import java.io.IOException;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import hw2.pagerank.UpdateCount;


public class Job1Reducer extends Reducer<Text, GraphNode, Text, GraphNode> {

	public void reduce(Text key, Iterable<GraphNode> values, Context context)
				throws IOException, InterruptedException {
			
			GraphNode node = new GraphNode();
			HashSet<String> uniqueLinks = new HashSet<String>();
			List<String> links=new LinkedList<String>();
		    
			// get the unique outlinks of the current node
			for(GraphNode gn:values) {
				for(String link:gn.getOutlinks())
					uniqueLinks.add(link);
			}
			
			for(String link:uniqueLinks)
				links.add(link);
			
			boolean isDangling = false;
			int size = links.size();
			
			// If the outlink size is 0, then it is a dangling node and we increment the 
			// global counter of dangling node by 1
			if(size==0) {
				isDangling = true;
				context.getCounter(UpdateCount.dangling_nodes).increment(1);
			}
			// we increment the total number of nodes by 1
			context.getCounter(UpdateCount.nodes).increment(1);
			
//			List<String> outlinkStrings = new LinkedList<String>();
//			for(String l:links)
//				outlinkStrings.add(l);
			
			// we set the appropriate node parameters and then write it to the reducer
			node.setPagerank(0);
			node.setOutlinks(links);
			node.setDangling(isDangling);
			
			// we output the pagename as the node with the pagerank and the adjacenecy list
			context.write(key, node);
		}
}
