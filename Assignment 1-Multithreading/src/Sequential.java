//package MyPackage;
import java.io.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;

class Fibonacci{
	
	public int fibonacci(int num){
		int prev=0,cur=1;
		for(int i=2;i<=num;i++){
			int sum = prev + cur;
			prev = cur;
			cur = sum;
		}
		return cur;
	}
	
}

public class Sequential {
	
	public static void main(String[] args) throws Exception {
		FileReader file = new FileReader(args[0]);
		BufferedReader reader = new BufferedReader(file);
		ArrayList<String> lines = new ArrayList<String>();
		String line = reader.readLine();
		
		// 1: Accumulate all the lines of the file in a List<>  
		while(line!=null){
			lines.add(line);
			line = reader.readLine();
		}
		
		ArrayList<Long> timings = new ArrayList<Long>();
		
		for(int i=0;i<10;i++){
		// The accumulation data structure where I stored all the TMAX temperatures by stationID 
			HashMap<String,ArrayList<Integer>> hmap = new HashMap<String,ArrayList<Integer>>();
			long startTime = System.currentTimeMillis();		
			
			for(String eachline:lines){
				String[] weatherData = eachline.split(",");
				String stationID = weatherData[0];
				String tempType = weatherData[2];
				int temp = Integer.parseInt(weatherData[3]);
				
				if(tempType.equals("TMAX")){
//					If the key already exists, then write it into the hashmap
					if(hmap.get(stationID)!=null){
							ArrayList<Integer> temps = hmap.get(stationID);
							ArrayList<Integer> newTemps = new ArrayList<Integer>();
														
							int sumOfTemps = temps.get(0);
							int numOfTemps = temps.get(1);
														
							sumOfTemps += temp;
							newTemps.add(sumOfTemps);
							numOfTemps += 1;
							newTemps.add(numOfTemps);
							
							hmap.put(stationID, newTemps);
					}
					else{
						ArrayList<Integer> temps = new ArrayList<Integer>();
						temps.add(temp);
						temps.add(1);
						hmap.put(stationID,temps);
					}
				}
			}
			
			// 2: sequential version for calculating average of TMAX temperatures by stationId
			for(String s:hmap.keySet()){
				ArrayList<Integer> record = hmap.get(s);
				int sum = record.get(0);
				int count = record.get(1);
				float average = (float)sum/count;
				System.out.println("average TMAX temperature for: "+s+" is : "+average);
			}
			
			long endTime = System.currentTimeMillis();
			long totalTimeTaken = endTime - startTime;

			
			timings.add(totalTimeTaken);
		}
		
		long max = Collections.max(timings);
		long min = Collections.min(timings);
		
		long sum=0;
		for(Long time:timings){
			sum += time;
		}
		
		System.out.println("MAX time: "+max+" ms "+"AVG time: "+(sum/10)+" ms "+"MIN time: "+min+" ms ");
		
		
		file.close();
	}

}
