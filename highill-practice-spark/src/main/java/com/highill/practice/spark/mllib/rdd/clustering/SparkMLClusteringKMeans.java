package com.highill.practice.spark.mllib.rdd.clustering;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;

import com.highill.practice.spark.JavaRDDSparkContextMain;

public class SparkMLClusteringKMeans {

	public static void main(String[] args) {
		JavaSparkContext javaSparkContext = JavaRDDSparkContextMain.javaSparkContext("SparkMLClusteringKMeans", "local[*]");
		String dataPath = "data/mllib/kmeans_data.txt";
		JavaRDD<String> fileData = javaSparkContext.textFile(dataPath);
		JavaRDD<Vector> parsedData = fileData.map(line -> {
			String[] stringArray = line.split(" ");
			double[] valueArray = new double[stringArray.length];
			for(int size = 0; size < stringArray.length; size++) {
				valueArray[size] = Double.valueOf(stringArray[size]);
			}
			Vector vector = Vectors.dense(valueArray);
			return vector;
			
		});
		System.out.println("-----fileData count: " + fileData.count());
		System.out.println("-----parseData count: " + parsedData.count());
		parsedData.cache();
		
		
		int numClusters = 2;
		int numIterations = 20;
		KMeansModel kMeansModel = KMeans.train(parsedData.rdd(), numClusters, numIterations);
		System.out.println("-----kMeansModel: " + kMeansModel);
		System.out.println("-----kMeansModel Cluster centers: ");
		for(Vector center : kMeansModel.clusterCenters()) {
			System.out.println("    " + center);
		}
		
		System.out.println("-----kMeansModel k: " + kMeansModel.k() + ",   toPMML: " + kMeansModel.toPMML());
		double cost = kMeansModel.computeCost(parsedData.rdd());
		System.out.println("-----cost: " + cost);
		
		double withinSetSumOfSquaredError = kMeansModel.computeCost(parsedData.rdd());
		System.out.println("-----withinSetSumOfSquaredError: " + withinSetSumOfSquaredError);
		System.out.println("-----kMeansModel k: " + kMeansModel.k() + ",   toPMML: " + kMeansModel.toPMML());

	}

}
