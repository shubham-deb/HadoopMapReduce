//package MyPackage;
import java.io.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

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

class MyThread16 implements Runnable{
	private static HashMap<String,ArrayList<Integer>> records = new HashMap<String,ArrayList<Integer>>();
	List<String> lines = new ArrayList<String>();
	
	public MyThread16(){	
	}
	
	public HashMap<String,ArrayList<Integer>> getRecords(){
		return records;
	}
	
	public MyThread16(List<String> subList) {
		this.lines = subList;
	}

	public void run(){
		for(String eachline:lines){
			String[] weatherData = eachline.split(",");
			String stationID = weatherData[0];
			String tempType = weatherData[2];
			int temp = Integer.parseInt(weatherData[3]);
			
			try{
				if(tempType.equals("TMAX")){
					
//				I have put a lock on the entire static hashmap 
					synchronized (records) {
						if(records.get(stationID)!=null){
								ArrayList<Integer> temps = records.get(stationID);
								ArrayList<Integer> newTemps = new ArrayList<Integer>();
								
								int sumOfTemps = temps.get(0);
								int numOfTemps = temps.get(1);
								
								sumOfTemps += temp;
								newTemps.add(sumOfTemps);
								numOfTemps += 1;
								newTemps.add(numOfTemps);
								
								records.put(stationID, newTemps);
								new Fibonacci().fibonacci(17);
						}
						else{
							ArrayList<Integer> temps = new ArrayList<Integer>();
							temps.add(temp);
							temps.add(1);
							records.put(stationID,temps);
							new Fibonacci().fibonacci(17);
						}
					}
				}
				
			}catch(Exception e){
				System.out.println("Exception occured "+e);
				e.printStackTrace();
			}
			
		}
	}
	
}

public class FiboCoarseLock {

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
		
		int numOfLines = lines.size()/2;
		
		ArrayList<Long> timings = new ArrayList<Long>();
		
		for(int i=0;i<10;i++){
			MyThread16 th1 = new MyThread16(lines.subList(0, numOfLines));
			MyThread16 th2 = new MyThread16(lines.subList(numOfLines, lines.size()));
			Thread t1 = new Thread(th1);
			Thread t2 = new Thread(th2);
			
			long startTime = System.currentTimeMillis(); 
			
			t1.start();
			t2.start();
			
			t1.join();
			t2.join();
				
			MyThread16 th = new MyThread16();
			HashMap<String,ArrayList<Integer>> records = th.getRecords();
			
			for(String s:records.keySet()){	
				ArrayList<Integer> record = records.get(s);
				int sum = record.get(0);
				int count = record.get(1);
//				System.out.println("Sum: "+sum+" count: "+count);
				float average = (float)sum/count;
				System.out.println("ID: "+s+" Average TMAX: "+average);
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
