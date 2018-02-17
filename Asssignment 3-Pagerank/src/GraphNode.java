package hw2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.io.WritableComparable;

public class GraphNode implements WritableComparable<GraphNode>{
	private double pagerank;
	private List<String> outlinks;
	private boolean isDangling;
	private double contributorsPRSum; 
	
	public GraphNode() {
		this.outlinks = new LinkedList<String>();
		this.isDangling = false;
	}
	
	public GraphNode(List<String> adjGraphNodes) {
		this.isDangling = false;
		this.outlinks = new LinkedList<String>();
		for(String outlinks:adjGraphNodes)
			this.outlinks.add(outlinks);
	}
	
	public GraphNode(boolean isDangling) {
		this.outlinks = new LinkedList<String>();
		this.isDangling = isDangling;
	}
	/**
	 * @return the pagerank
	 */
	public double getPagerank() {
		return pagerank;
	}
	/**
	 * @param pagerank the pagerank to set
	 */
	public void setPagerank(double pagerank) {
		this.pagerank = pagerank;
	}
	/**
	 * @return the outlinks
	 */
	public List<String> getOutlinks() {
		return outlinks;
	}
	/**
	 * @param outlinks the outlinks to set
	 */
	public void setOutlinks(List<String> outlinks) {
		this.outlinks.clear();
		for(String links:outlinks)
			this.outlinks.add(links);
	}

	/**
	 * @return the contributors
	 */
	public double getContributorsPRSum() {
		return contributorsPRSum;
	}

	/**
	 * @param contributors the contributors to set
	 */
	public void setContributorsPRSum(double contributorsPRSum) {
		this.contributorsPRSum = contributorsPRSum;
	}

	@Override
	public String toString() {
		return this.pagerank + "," + this.outlinks;
	}

	/**
	 * @return the isDangling
	 */
	public boolean isDangling() {
		return isDangling;
	}
	/**
	 * @param isDangling the isDangling to set
	 */
	public void setDangling(boolean isDangling) {
		this.isDangling = isDangling;
	}

	public void write(DataOutput out) throws IOException {
        out.writeBoolean(isDangling);
        out.writeDouble(pagerank);
        out.writeDouble(contributorsPRSum);
        out.writeInt(outlinks.size());
        for(int i = 0 ; i < outlinks.size(); i++){
            out.writeUTF(outlinks.get(i));
        }
	}

	public void readFields(DataInput in) throws IOException {
	      isDangling = in.readBoolean();
	      pagerank = in.readDouble();
	      contributorsPRSum = in.readDouble();
	      int num_of_nodes = in.readInt();
	      outlinks = new LinkedList<String>();
	       for(int i = 0 ; i< num_of_nodes; i++){
	    	   outlinks.add(in.readUTF());
	       }
	}

	public int compareTo(GraphNode o) {
		return 0;
	}

}
