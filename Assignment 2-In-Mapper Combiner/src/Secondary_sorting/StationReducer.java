package pckg4;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class StationReducer extends Reducer<StationYearKey,Text,Text,Text>{
	public HashMap<String,HashMap<String,String>> hmap;
//	private static final Log _log = LogFactory.getLog(StationReducer.class);
	private static final int FLUSH_LIMIT = 50000;
	
	// create a hashmap whenever reducer is called
	protected void setup(Context context)
            throws IOException,InterruptedException{
		hmap = new LinkedHashMap<String,HashMap<String,String>>();
	}
	
	public void reduce(StationYearKey key, Iterable<Text> values,Context context ) 
			throws IOException, InterruptedException {
		
//		Text keyToString = new Text(key.toString());
//		Here the records will come in ascending order of keys with the earliest year and stationId.
//		eg: reord (1122,1080) will come before (1123,1180) since we have implemented grouping comparator
// 		and inside the compsite class we have defined the compareTo method which will sort based on composite key.
		int minSum=0,maxSum=0,minCount=0,maxCount=0;
		for(Text val:values) {
			String[] arr = val.toString().split(",");
			int temp = Integer.parseInt(arr[1]);
			
			if(arr[0].equals("TMAX")) {
				maxSum+=temp;
				maxCount++;
			}else if(arr[0].equals("TMIN")) {
				minSum+=temp;
				minCount++;
			}
		}
				
		float meanMin = 0,meanMax=0;
		
		if(minCount!=0) {
			meanMin = (float)minSum/minCount ;
		}else {
			meanMin = 0;
		}
		
		if(maxCount!=0) {
			meanMax = (float)maxSum/maxCount;
		}else {
			meanMax = 0;
		}
		
		String stationId = key.getStationId();
		String year = key.getYear();
		
		// if the values of stationId is not null, then append the new values
		if(hmap.get(stationId)!=null) {
			hmap.get(stationId).put(year,""+meanMax+","+meanMin);
		}
		// else create new values and put them in the hashmap
		else {
			HashMap<String,String> vals = new LinkedHashMap<>();
			vals.put(year, ""+meanMax+","+meanMin);
			hmap.put(stationId, vals);
		}
		
		// checks if the hashmap ran out of memory
		// if it did then we write it to file and clear the hashmap
		if(check_limit(context,false)){
			for(String k:hmap.keySet()) {
				HashMap<String,String> vals = hmap.get(k);
				String result="";
				for(String k2:vals.keySet()) {
					result += "("+k2+","+vals.get(k2)+")";
				}
				context.write(new Text(k+","),new Text("["+result+"]"));
			}
			hmap.clear();
		}
	}
	
	// checks if the hashmap has reached it's limit
	private boolean check_limit(Context context, boolean force) 
			throws IOException, InterruptedException {
		if(!force) {
			int size = hmap.size();
			if(size<FLUSH_LIMIT) {
				return false;
			}
		}
		return true;
	}
	
	// called before the reducer ends
	protected void cleanup(Context context)
            throws IOException,InterruptedException{
		for(String k:hmap.keySet()) {
			HashMap<String,String> vals = hmap.get(k);
			String result="";
			for(String k2:vals.keySet()) {
				result += "("+k2+","+vals.get(k2)+")";
			}
			context.write(new Text(k+","),new Text("["+result+"]"));
		}
	}
	
}
