package com.highill.practice.spark.mllib.rdd.clustering;

import java.util.ArrayList;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.clustering.BisectingKMeans;
import org.apache.spark.mllib.clustering.BisectingKMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;

import com.google.common.collect.Lists;
import com.highill.practice.spark.JavaRDDSparkContextMain;

/**
 * 
 * http://spark.apache.org/docs/2.1.0/mllib-clustering.html#bisecting-k-mean
 *
 */
public class SparkMLClusteringBisectingKMeans {

	public static void main(String[] args) {
		JavaSparkContext javaSparkContext = JavaRDDSparkContextMain.javaSparkContext("SparkMLClusteringBisectingKMeans", "local[*]");
		
		ArrayList<Vector> baseData = Lists.newArrayList(
				Vectors.dense(0.1, 0.1), Vectors.dense(0.3, 0.3), Vectors.dense(0.25, 0.25), Vectors.dense(0.115, 0.125),
				Vectors.dense(10.1, 10.1), Vectors.dense(10.3, 10.3),
				Vectors.dense(20.1, 20.1), Vectors.dense(20.3, 20.3),
				Vectors.dense(30.1, 30.1), Vectors.dense(30.3, 30.3), Vectors.dense(30.2, 30.222)
				);
		
		JavaRDD<Vector> trainingData = javaSparkContext.parallelize(baseData, 2);
		System.out.println("-----trainingData count: " + trainingData.count());
		
		int k = 4;
		BisectingKMeans bisectingKMeans = new BisectingKMeans().setK(k);
		BisectingKMeansModel bisectingKMeansModel = bisectingKMeans.run(trainingData);
		System.out.println("-----bisectingKMeansModel: " + bisectingKMeansModel);
		double computeCost = bisectingKMeansModel.computeCost(trainingData);
		System.out.println("-----computeCost: " + computeCost);
		
		Vector[] clusterCenterArray = bisectingKMeansModel.clusterCenters();
		for(int size = 0; size < clusterCenterArray.length; size++) {
			Vector clusterCenter = clusterCenterArray[size];
			System.out.println("-----Cluster Center " + size + ": " + clusterCenter);
		}

	}

}
