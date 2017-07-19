package com.highill.practice.spark.streaming;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

import com.highill.practice.spark.JavaRDDSparkContextMain;

/**
 * 
 * window length - The duration of the window  
 * sliding interval - The interval at which the window operations performed  
 *
 */
public class SparkStreamingClient {
	

	public static void main(String[] args) {

		// method 1
		// JavaSparkContext sparkContext = JavaRDDSparkContextMain.javaSparkContext("Spark Streaming Cient", "local[*]");
		// JavaStreamingContext streamingContext = new JavaStreamingContext(sparkContext, new Duration(1000));

		// Exception in thread "main" org.apache.spark.SparkException: Only one SparkContext may be running in this JVM (see SPARK-2243). 
		// To ignore this error, set spark.driver.allowMultipleContexts = true.
		// The currently running SparkContext was created at:
		// method 2
		SparkConf sparkConf = JavaRDDSparkContextMain.sparkConf("Spark Streaming Cient", "local[*]");
	    JavaStreamingContext streamingContext = new JavaStreamingContext(sparkConf, new Duration(1000));
		System.out.println("-----streamingContext: " + streamingContext);
		
		// need linux install nc command
		// CentOS: yum -y install nmap-ncat; nc -lk 9999 
		// input string streaming to Iterator<String>
		JavaReceiverInputDStream<String> streamingLine = streamingContext.socketTextStream("192.168.1.105", 9999);
		
		JavaDStream<String> streamingWords = streamingLine.flatMap(input -> {
			String[] wordArray = input.split(" ");
			List<String> wordList = Arrays.asList(wordArray);
			return wordList.iterator();
		});
		
		System.out.println("-----print streamingWords: ");
		streamingWords.print();
		
		// 
		JavaPairDStream<String, Integer> wordTuple = streamingWords.mapToPair(word -> {
			Tuple2<String, Integer> tuple2 = new Tuple2<String, Integer>(word, 1);
			return tuple2;
		});
		System.out.println("-----wordTuple count: " + wordTuple.count() + ", wordTuple: " + wordTuple);
		wordTuple.print();
		
		// work count 
		JavaPairDStream<String, Integer> wordCount = wordTuple.reduceByKey((v1, v2) -> {
			return v1 + v2;
		});
		System.out.println("-----wordCount count: " + wordCount.count() + ", wordCount:  " + wordCount);
		wordCount.print();
		
		
		
		
		System.out.println("-----streamingWords: " + streamingWords.toString());
		streamingContext.start();
		
		
		try{
			// streamingContext.awaitTermination();
			
			Thread.sleep(1000 * 30);
		} catch(Exception e) {
			e.printStackTrace();
		}

		streamingContext.stop();
		streamingContext.close();

	}

}
