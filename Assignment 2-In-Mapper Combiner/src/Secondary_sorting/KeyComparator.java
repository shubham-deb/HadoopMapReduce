package pckg4;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

//public class KeyComparator extends WritableComparator{
//	
//	@Override
//	public int compare(WritableComparable w1, WritableComparable w2) {
//		System.out.println("in key comparator");
//
//		Text k1 = (Text)w1;
//		String[] key1 = k1.toString().split(",");
//		String stationId_1 = key1[0];
//		String year_1 = key1[1];
//		
//		Text k2 = (Text)w2;
//		String[] key2 = k2.toString().split(",");
//		String stationId_2 = key2[0];
//		String year_2 = key2[1];
//		
//		int result = stationId_1.compareTo(stationId_2);
//		
////		if the stations are the same sort by year in ascending order
//		if(result == 0) {
//			result = year_1.compareTo(year_2);
//		}
//		
//		return result;
//	}
//}

public class KeyComparator extends WritableComparator {
	 
	 protected KeyComparator() {
	 super(StationYearKey.class, true);
	 }
	 
	 @Override
	 public int compare(WritableComparable w1, WritableComparable w2) {
	 
	//ascending stationId and year
	 
	 StationYearKey t1 = (StationYearKey) w1;
	 StationYearKey t2 = (StationYearKey) w2;
//	 String[] key_1 = t1.toString().split(",");
//	 String[] key_2 = t2.toString().split(",");
//
//	 String stationId_1 = key_1[0];
//	 String stationId_2 = key_2[0];
	 int comp = t1.getStationId().compareTo(t2.getStationId());
	 
	//ascending value of year
	 if (comp == 0) {
		 comp = t1.getYear().compareTo(t2.getYear());
	 }
	 
	 return comp;
	 
	}
}