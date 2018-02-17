package pckg4;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class StationYearTempMapping extends Mapper<Object, Text, StationYearKey, Text>{
	
//	private static final Log _log = LogFactory.getLog(StationYearTempMapping.class);
	
	public void map(Object key,Text values,Context context) 
			throws IOException, InterruptedException{
		Text temp = new Text();
		String[] stationInfo = values.toString().split(",");
		
		if(stationInfo[2].equals("TMAX") || stationInfo[2].equals("TMIN")) {
			
			StationYearKey compositeKey = new StationYearKey(stationInfo[0],stationInfo[1].substring(0,4));
			String tempInfo = stationInfo[2] +","+ stationInfo[3];
			temp.set(tempInfo);
		
			// we emit the stationId and the year as the key and "TMAX or TMIN" with the 
			// temperature as the value
			context.write(compositeKey,temp);
//			_log.debug(stationIdYear + " => " + temp);
		}
	}
}
