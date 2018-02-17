## Goals
*  For MapReduce, compare Combiners to the in-mapper combining design pattern. Use keys, comparators, and Partitioner to implement secondary sort. Then explore how this would be implemented in Spark Scala.


# Assignment 2 - In-Mapper Combiner 

* In this assignment, I am analysing performance of the following design patterns on weather dataset: * No-Combiner Approach/Simple Map-Reduce * Combiner Design pattern * In-Mapper Combining Design Pattern * Secondary Sort / Value-to-key conversion design pattern

* This data consists of weather readings from multiple weather stations. The format and description of each fields is given in the following link ftp://ftp.ncdc.noaa.gov/pub/data/ghcn/daily/by_year/readme.txt

* First, I am calculating the mean minimum temperature and the mean maximum temperature, by station, for a single year of data using three design patterns.
  * Reducer Output Format (lines do not have to be sorted by StationID):
	* StationId0, MeanMinTemp0, MeanMaxTemp0
	* StationId1, MeanMinTemp1, MeanMaxTemp1
	* …
	* StationIdn, MeanMinTempn, MeanMinTempn

	* NoCombiner: This program has a Mapper and a Reducer class with no custom setup or cleanup methods, and no Combiner or custom Partitioner. 

	* Combiner: This version of the program uses a Combiner.

	* InMapperCombiner: This version of the program uses in-mapper combining to reduce data traffic from Mappers to Reducers.

* Second, using 10 years of input data (1880.csv to 1889.csv), I have to calculate mean minimum temperature and mean maximum temperature, by station, by year using another design pattern: 

	* Secondary_sorting: This version of the program uses 10 years of input data (1880.csv to 1889.csv), and calculates mean minimum temperature and mean maximum
	temperature, by station, by year. Using the secondary sort design pattern minimizes the amount of memory utilized during Reduce function execution.

* Ran all three MapReduce programs from 1 above in Elastic MapReduce (EMR) on the unzipped climate data from 1991.csv, using six m4.large machines (1 master, 5 workers).

* Ran the secondary sort in Elastic MapReduce (EMR) on the unzipped climate data from 1880.csv to 1889.csv, using six m4.large machines (1 master, 5 workers).

* I wrote a brief report about my findings in report.odt.

## Author
* **Shubham Deb**
