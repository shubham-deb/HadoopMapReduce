//package MyPackage;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;


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

class MyThread15 implements Runnable{
	public static ConcurrentHashMap<String,ArrayList<Integer>> records = new ConcurrentHashMap<String,ArrayList<Integer>>();
	List<String> lines = new ArrayList<String>();
//	Tid is used to lock same threads trying to run at the same time
	String tid;
	
	public MyThread15(){	
	}
	
	public MyThread15(List<String> subList,String id) {
		this.lines = subList;
		this.tid = id;
	}

	public ConcurrentHashMap<String,ArrayList<Integer>> getRecords(){
		return records;
	}
	
	public void run(){
		for(String eachline:lines){
			String[] weatherData = eachline.split(",");
			String stationID = weatherData[0];
			String tempType = weatherData[2];
			int temp = Integer.parseInt(weatherData[3]);
			
			ArrayList<Integer> rec = new ArrayList<Integer>();
			rec.add(temp);
			rec.add(1);
			
			ArrayList<Integer> newTemps = new ArrayList<Integer>();
			
			try{
				if(tempType.equals("TMAX")){
//					Here I will put a synchronized block on the values of the stationId, so 
//					I will put a lock on each record using the key
					if(records.get(stationID)!=null){
						synchronized (records.get(stationID)) {
							
								int sumOfTemps = records.get(stationID).get(0);
								int numOfTemps = records.get(stationID).get(1);
								
								sumOfTemps += temp;
								newTemps.add(sumOfTemps);
								numOfTemps += 1;
								newTemps.add(numOfTemps);
								
								records.put(stationID, newTemps);
								new Fibonacci().fibonacci(17);
							}
					}
						else{
//					Here If two threads which have the same tid are entering at the same time,
//					only one thread will be running at the same time
							synchronized (this.tid) {
//								Here If the new thread which already has the stationId tries to enter
//								but another thread has already written on the same stationId, we check 
//								it using containsKey
								if(records.containsKey(stationID)){
									synchronized (records.get(stationID)) {
										int sumOfTemps = records.get(stationID).get(0);
										int numOfTemps = records.get(stationID).get(1);
										
										sumOfTemps += temp;
										newTemps.add(sumOfTemps);
										numOfTemps += 1;
										newTemps.add(numOfTemps);
										
										records.put(stationID, newTemps);
									}
								}
								else{
									records.put(stationID,rec);
								}
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

public class FiboFineLock {

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

		int numOfLines = lines.size()/2;
		
		ArrayList<Long> timings = new ArrayList<Long>();
		
		for(int i=0;i<10;i++){
		
			MyThread15 th1 = new MyThread15(lines.subList(0, numOfLines),"1");
			MyThread15 th2 = new MyThread15(lines.subList(numOfLines, lines.size()),"2");
			Thread t1 = new Thread(th1);
			Thread t2 = new Thread(th2);
			
			long startTime = System.currentTimeMillis(); 
			
			t1.start();
			t2.start();
			
			try{
			t1.join();
			}catch(Exception e){
				System.out.println(e);
			}
			
			t2.join();
			
			MyThread15 obj = new MyThread15();
			ConcurrentHashMap<String,ArrayList<Integer>> records = obj.getRecords();
			
			for(String stationId:records.keySet()){
				ArrayList<Integer> record = records.get(stationId);
				int sum = record.get(0);
				int count = record.get(1);
				float average = (float)sum/count;
				System.out.println("ID: "+stationId+" Average TMAX: "+average);
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
