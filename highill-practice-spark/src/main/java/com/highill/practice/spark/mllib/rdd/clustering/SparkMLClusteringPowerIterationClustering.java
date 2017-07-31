package com.highill.practice.spark.mllib.rdd.clustering;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.clustering.PowerIterationClustering;
import org.apache.spark.mllib.clustering.PowerIterationClusteringModel;

import scala.Tuple3;

import com.google.common.collect.Lists;
import com.highill.practice.spark.JavaRDDSparkContextMain;

public class SparkMLClusteringPowerIterationClustering {

	public static void main(String[] args) {
		JavaSparkContext javaSparkContext = JavaRDDSparkContextMain.javaSparkContext("SparkMLClusteringPowerIterationClustering", "local[*]");
		JavaRDD<Tuple3<Long, Long, Double>> similaritiesData = javaSparkContext.parallelize(Lists.newArrayList(
				new Tuple3<>(0L, 1L, 0.9),
				new Tuple3<Long, Long, Double>(1L, 2L, 0.9),
				new Tuple3<Long, Long, Double>(2L, 3L, 0.9),
				new Tuple3<Long, Long, Double>(3L, 4L, 0.9),
				new Tuple3<Long, Long, Double>(4L, 5L, 0.9)
				));
		System.out.println("-----similaritiesData count: " + similaritiesData.count());  
		
		PowerIterationClustering powerIterationClustering = 
				new PowerIterationClustering().setK(2).setMaxIterations(10);
		PowerIterationClusteringModel powerIterationClusteringModel = 
				powerIterationClustering.run(similaritiesData);
		System.out.println("-----powerIterationClusteringModel: " + powerIterationClusteringModel);
		
		for(PowerIterationClustering.Assignment assignment : powerIterationClusteringModel.assignments().toJavaRDD().collect()) {
			System.out.println("-----assignment id: " + assignment.id() + "  ->  " + assignment.cluster());
		}
		
		

	}

}
