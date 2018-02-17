package pckg4;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class CustomPartitioner extends Partitioner<StationYearKey, Text>{

	@Override
	public int getPartition(StationYearKey key, Text value, int NumReduceTasks) {
//		int hash = key.getStationId().hashCode();
		int partition = Math.abs(key.toString().hashCode())%NumReduceTasks;
		return partition;
	}
	
}
