# Parellel Data Processing with MapReduce

This repository contains the source codes & scripts of my Master's level course - CS6240 Parallel Data Processing in Map-Reduce course at College of Computer & Information Science, Northeastern University, Boston MA.
Hadoop MapReduce is a software framework for easily writing applications which process vast amounts of data (multi-terabyte data-sets) in-parallel on large clusters (thousands of nodes) of commodity hardware in a reliable, fault-tolerant manner.

## Getting Started
 * I recommend using Linux for Hadoop development. (I had problems with Hadoop on Windows.) If your computer is a Windows machine, you can run Linux in a virtual machine. I tested Oracle VirtualBox and created a virtual machine running Linux, e.g., Ubuntu (free). 
 * If you are using a virtual machine, then you need to apply the following steps to the virtual machine.
Download a Hadoop 2 distribution, e.g., version 2.7.3, directly from http://hadoop.apache.org/ and
unzip it in your preferred directory, e.g., /usr/local. That’s almost all you need to do to be able to run
Hadoop code in standalone (local) mode from your IDE, e.g., Eclipse or IntelliJ. Make sure your IDE
supports development in Java. Java 1.7 and 1.8 should both work.
 * In your IDE, you should create a Maven project. This makes it simple to build “fat jars”, which recursively
include dependent jars used in your MapReduce program. There are many online tutorials for installing
Maven and also creating Maven projects via archetypes. These projects can be imported into your IDE or
built from a shell.

### Running a sample WordCount Program
* Find the example Word Count code in Apache Hadoop https://hadoop.apache.org/docs/stable/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html#Purpose
* Follow these steps if you want to run the program using CLI.https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/SingleCluster.html
* To configure ECLIPSE for HADOOP, you have to import Hadoop jar files that you need in your maven repository. for example for mapreduce use the following link:https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-mapreduce-client-core
Other hadoop APIs for development can be found here: https://mvnrepository.com/artifact/org.apache.hadoop
Then you will need to provide an input directory containing text files and a path to an output directory and pass it as an argument to the WordCount program.

## Author
* **Shubham Deb**
