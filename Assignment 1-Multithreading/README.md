## Goals
* Gain hands-on experience with parallel computation and synchronization primitives in a single machine shared-memory environment.

# Assignment 1 - Multithreading 

* This assignment evaluates the performance of various multi-threading approaches against the sequential approach. In this assignment we are using the data from
  ftp://ftp.ncdc.noaa.gov/pub/data/ghcn/daily/by_year/

* This data consists of weather readings from multiple weather stations. The format and description of each fields is given in the following link ftp://ftp.ncdc.noaa.gov/pub/data/ghcn/daily/by_year/readme.txt

* In this assignment , we are evaluating various multithreading methods by calculating average TMAX value from file 1912.csv located at ftp://ftp.ncdc.noaa.gov/pub/data/ghcn/daily/by_year/ .

* In order to see the effects of multi-threading on large dataset, We will be adding a function fib(17) which calculates 17th fibonacci number to slow down the updates in the value accumulation data structure. This function is present in each of the approaches.
	
	* NoLock : Consits of code where I have implemented the Multi-threaded version that assigns subsets of the input String[]  (or List<String>) for concurrent processing by separate threads. This version uses a single shared accumulation data structure and should use no locks or synchronization on it, i.e., it completely ignores any possible data inconsistency due to parallel execution.
	* CoarseLock : Consits of code where I have implemented Multi-threaded version that assigns subsets of the input String[]  (or List<String>) for processing by separate threads. This version uses a single shared accumulation data structure and a single lock on the entire data structure.
	* FineLock :  Consits of code where I have implemented the Multi-threaded version that assigns subsets of the input String[]  (or List<String>) for processing by separate threads. This version should also use a single shared accumulation data structure, but should lock only the accumulation value objects and not the whole data structure. 
	* NoSharing : Consits of code where I have implemented the Per-thread data structure multi-threaded version that assigns subsets of the input String[]  (or List<String>) for processing by separate threads. Each thread works on its own separate instance of the accumulation data structure. Hence no locks were needed. However, I needed a barrier to determine when the separate threads have terminated and then reduce the separate data structures into a single one using
	the main thread.
	* Sequential : Consists of Sequential version that calculates the average of the TMAX temperatures by station Id.

* All the other files have a function fib(17) added to their respective versions to slow down any updates to the value accumulation data structure.


## Author
* **Shubham Deb**
