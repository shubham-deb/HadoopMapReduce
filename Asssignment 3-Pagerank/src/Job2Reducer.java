package hw2;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import hw2.pagerank.UpdateCount;

public class Job2Reducer extends Reducer<Text, GraphNode, Text, GraphNode>{
	
	double pagecount;
	int iteration;
	double alpha;
	double constantTerm;
	double danglingNodesSum;
	static double newDanglingNodesSum;
	static double newPagerankSum;
	
	public void setup(Context context){
		newDanglingNodesSum = 0;
		newPagerankSum=0;
		context.getCounter(UpdateCount.dangling_nodes_sum).setValue(0);
		Configuration conf = context.getConfiguration();
		iteration = conf.getInt("iteration", -1);
		alpha = conf.getDouble("alpha", -1);
		pagecount = conf.getDouble("numnodes", 0);
		danglingNodesSum = conf.getDouble("dangling_nodes_sum", 0);
		// multiplied by 10^9 to get upto 9 digits of the dangling node sum
		// and then update that in the counter as it only takes long values
		
		if(iteration==-1 || pagecount==-1 || alpha==-1)
			throw new Error("values are wrong");
		
		// we calculate part of the total pagerank of the current node
		if(iteration == 0)
			constantTerm = ((double)1/pagecount);
		else
			constantTerm = ((alpha/pagecount) + (1-alpha)*(danglingNodesSum/pagecount));
	}
	
	public void reduce(Text key, Iterable<GraphNode> values, Context context)
			throws IOException, InterruptedException {
		GraphNode gnode = new GraphNode();
		double pageRankContribSum=0d;
		
		// If it is the initial iteration, we don’t need to calculate the
		// neighboring contributions so we just emit the node with the
		// pagerank as constantTerm
		if(iteration==0) {
			gnode.setPagerank(constantTerm);
		      for (GraphNode node : values) {
		            if (!node.isDangling()) {
		                //Sum the contribution from the other pages
		            	gnode.setOutlinks(node.getOutlinks());
		            } 
		        }
		      
		      	newPagerankSum+=constantTerm;
		      
		      	// If it is the dangling node then we increment the dangling
				// node Pagerank sum
		        if (gnode.getOutlinks().size() == 0) {
		        	newDanglingNodesSum+=constantTerm;
		            context.getCounter(UpdateCount.dangling_nodes_sum).increment((long)(constantTerm* Math.pow(10, 9)));
		        }
		        
		     // emit the node with the pagerank
		        context.write(key,gnode);
		        
		}
		// For the other iterations we need to calculate the contributions
		// from other nodes and incorporate into the node’s pagerank
		else {
	      for (GraphNode node : values) {

	            if (node.isDangling()) {
	                //Sum the contribution from the other pages
	            	pageRankContribSum += node.getContributorsPRSum();
	            } else {
	            	
	            	// set the outlinks of the current node
	            	// bcz we need to check for dangling nodes in the graph
//	    			List<String> outlinkStrings = new LinkedList<String>();
//	    			List<Text> outlinkTexts = node.getOutlinks();
//	    			for(Text l:outlinkTexts)
//	    				outlinkStrings.add(l.toString());
	    			gnode.setOutlinks(node.getOutlinks());
	            }
	        }

	        // New Page Rank
	        double newPageRank = (constantTerm + (1 - alpha) * pageRankContribSum);
//	        DecimalFormat df = new DecimalFormat("#");
//	        df.setMaximumFractionDigits(8);
//	        newPageRank = Double.parseDouble(df.format(newPageRank));
//	        BigDecimal pr = new BigDecimal(newPageRank);
	        
	        gnode.setPagerank(newPageRank);

	        // If current node is dangling node then increment global counter which keeps the track of
	        // sum of page rank of dangling node.
	        if (gnode.getOutlinks().size() == 0) {
	        	newDanglingNodesSum+=newPageRank;
	            context.getCounter(UpdateCount.dangling_nodes_sum).increment((long)(newPageRank* Math.pow(10, 9)));
	        }

//	        context.getCounter(UpdateCount.total_pr_sum).increment((long)(newPageRank* Math.pow(10, 9)));
//	        System.out.println("pagerank of "+key+" is "+gnode.getPagerank());
	        newPagerankSum+=newPageRank;
	        // Emit the node which has new page rank and adjacency page list
	        context.write(key,gnode);
		}
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.mapreduce.Reducer#cleanup(org.apache.hadoop.mapreduce.Reducer.Context)
	 */
	@Override
	protected void cleanup(Reducer<Text, GraphNode, Text, GraphNode>.Context context)
			throws IOException, InterruptedException {
		Configuration conf  = context.getConfiguration();
		conf.setDouble("dangling_nodes_sum", newDanglingNodesSum);
//		System.out.println("New dangling node sum "+conf.getDouble("dangling_nodes_sum",0));
//		System.out.println("Pagerank sum "+newPagerankSum);
	}
}
