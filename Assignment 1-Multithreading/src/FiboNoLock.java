//package MyPackage;

import java.io.BufferedReader;
import java.io.FileReader;
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

class MyThread10 implements Runnable{
	public static HashMap<String,ArrayList<Integer>> records = new HashMap<String,ArrayList<Integer>>();
	List<String> lines = new ArrayList<String>();
	
	public MyThread10(){	
	}
	
	public HashMap<String,ArrayList<Integer>> getRecords(){
		return records;
	}
	
	public MyThread10(List<String> subList) {
		this.lines = subList;
	}

	public void run(){
//		System.out.println("Thread "+Thread.currentThread().getName()+" is running");
		putDataIntoRecord(lines);
//		average = calculateAverage(records);
	}
	
	public static void  putDataIntoRecord(List<String> lines){
		
		for(String eachline:lines){
			String[] weatherData = eachline.split(",");
			String stationID = weatherData[0];
			String tempType = weatherData[2];
			int temp = Integer.parseInt(weatherData[3]);
			
			try{
				if(tempType.equals("TMAX")){
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
				
			}catch(Exception e){
				System.out.println("Exception occured "+e);
				e.printStackTrace();
			}
			
		}
	}
	
}

public class FiboNoLock {

	public static void main(String[] args)throws Exception {
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
		
		int numOfLines = (lines.size()/2)-1;
		
		ArrayList<Long> timings = new ArrayList<Long>();
		
		for(int i=0;i<10;i++){
			MyThread10 th1 = new MyThread10(lines.subList(0, numOfLines));
			MyThread10 th2 = new MyThread10(lines.subList(numOfLines, lines.size()));
			Thread t1 = new Thread(th1);
			Thread t2 = new Thread(th2);
			
			long startTime = System.currentTimeMillis(); 
			
			t1.start();
			t2.start();
			
			t1.join();
			t2.join();
	
			MyThread10 th = new MyThread10();
			HashMap<String,ArrayList<Integer>> records = th.getRecords();
			
			for(String s:records.keySet()){	
				ArrayList<Integer> record = records.get(s);
				int sum = record.get(0);
				int count = record.get(1);
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
