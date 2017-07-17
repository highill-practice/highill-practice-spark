package com.highill.practice.spark;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class JavaRDDSparkContextMain {

	public static JavaSparkContext javaSparkContext(String appName, String master) {
		JavaSparkContext sparkContext = null;
		if (appName != null && master != null) {
			SparkConf sparkConf = new SparkConf();
			sparkConf.setAppName(appName);
			sparkConf.setMaster(master);

			sparkContext = new JavaSparkContext(sparkConf);
		}

		return sparkContext;
	}

	public static void main(String[] args) {
		// init Spark context
		String appName = "highill-practice-spark";
		// String master = "spark://127.0.0.1:7077";
		String master = "local[*]";

		JavaSparkContext sparkContext = javaSparkContext(appName, master);

		List<Integer> integerList = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 1, 2, 3, 4);

		JavaRDD<Integer> rddInteger = sparkContext.parallelize(integerList);
		System.out.println("-----rddInteger: " + rddInteger);
		Integer reduceResult = rddInteger.reduce((a, b) -> (a + b));
		System.out.println("-----reduceResult: " + reduceResult);

		System.out.println("-----rddInteger collect: " + rddInteger.collect());
		System.out.println("-----rddInteger count: " + rddInteger.count());
		System.out.println("-----rddInteger first: " + rddInteger.first());
		System.out.println("-----rddInteger take 2: " + rddInteger.take(2));
		System.out.println("-----rddInteger countByKey: " + rddInteger.countByValue());

		System.out.println("-----rddInteger map: " + rddInteger.map(v -> ("map" + v)).collect());
		
		JavaRDD<String> rddFlatMap = rddInteger.flatMap(v -> {
			Set<String> set = new HashSet<String>();
			set.add("flatMap" + v);
			return set.iterator();
		});
		System.out.println("-----rddInteger flatMap: " + rddFlatMap.collect());
		
		

		sparkContext.close();

	}

}
