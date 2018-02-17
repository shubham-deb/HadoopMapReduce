//package MyPackage;
import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;


class MyThread5 implements Runnable{
	private HashMap<String,ArrayList<Integer>> records = new HashMap<String,ArrayList<Integer>>();
	List<String> lines = new ArrayList<String>();

	public MyThread5(){	
	}
	
	public MyThread5(List<String> subList) {
		this.lines = subList;
	}
	
	public HashMap<String,ArrayList<Integer>> getRecords(){
		return records;
	}
	
	public void run(){
//		I will get the key and value pairs for each input text
		records = putDataIntoRecord(lines);
	}
	
	public static HashMap<String,ArrayList<Integer>> putDataIntoRecord(List<String> lines){
		
//		A local hashmap for each thread
		HashMap<String,ArrayList<Integer>> hmap = new HashMap<String,ArrayList<Integer>>();
		for(String eachline:lines){
			String[] weatherData = eachline.split(",");
			String stationID = weatherData[0];
			String tempType = weatherData[2];
			int temp = Integer.parseInt(weatherData[3]);
			
			ArrayList<Integer> tmps = new ArrayList<Integer>();
			tmps.add(temp);
			tmps.add(1);
			
			try{
				if(tempType.equals("TMAX")){
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
		//						If this is the first entry of a station insert into the hashmap 
		//						with the count and the sum
								hmap.put(stationID,tmps);
								}
				}
				
			}catch(Exception e){
				System.out.println("Exception occured "+e);
				e.printStackTrace();
			}
		}
		return hmap;
	}
	
}
public class NoSharing {

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
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
		int numOfLines = lines.size()/2;
		
		for(int i=0;i<10;i++){
			
			long startTime = System.currentTimeMillis();
			
			MyThread5 th = new MyThread5(lines.subList(0, numOfLines));
			Thread t1 = new Thread(th);
			t1.start();
			t1.join();
			
			MyThread5 th2 = new MyThread5(lines.subList(numOfLines, lines.size()));
			Thread t2 = new Thread(th2);
			t2.start();
			t2.join();
			
			HashMap<String,ArrayList<Integer>> intermediate = th.getRecords();
			HashMap<String,ArrayList<Integer>> intermediate2 = th2.getRecords();
			
			HashMap<String,ArrayList<Integer>> result = new HashMap<String,ArrayList<Integer>>();
			
	//		MERGE THESE TWO HASHMAPS TO GET ONE HASHMAP FOR EACH STATIONID
			for(String station:intermediate.keySet()){
				result.put(station, intermediate.get(station));
			}
			
			for(String station:intermediate2.keySet()){
				ArrayList<Integer> oldValues = result.get(station);
				if(oldValues!=null){
					ArrayList<Integer> newValues = intermediate2.get(station);
					int sum = oldValues.get(0) + newValues.get(0);
					int count = oldValues.get(1) + newValues.get(1);
					result.put(station, new ArrayList<>(Arrays.asList(sum,count)));
				}
				else{
					result.put(station, intermediate2.get(station));
				}
			}
			
			for(String stationId:result.keySet()){
				ArrayList<Integer> record = result.get(stationId);
				int sum = record.get(0);
				int count = record.get(1);
				float average = (float)sum/count;
				System.out.println("ID: "+stationId+" Average Temperature: "+average);
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
