package pckg4;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
public class GroupingComparator extends WritableComparator {
	 
	 protected GroupingComparator() {
	 super(StationYearKey.class, true);
	 }
	 
	 @Override
	 public int compare(WritableComparable w1, WritableComparable w2) {
	 
	//consider only stationId as part of the key
	 StationYearKey t1 = (StationYearKey) w1;
	 StationYearKey t2 = (StationYearKey) w2;
	 int result = t1.getYear().compareTo(t2.getYear());
	 
	 return result;
	 	
	 }
}