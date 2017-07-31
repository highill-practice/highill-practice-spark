package com.highill.practice.spark.mllib.rdd.clustering;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.clustering.GaussianMixture;
import org.apache.spark.mllib.clustering.GaussianMixtureModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;

import com.highill.practice.spark.JavaRDDSparkContextMain;

public class SparkMLClusteringGaussianMixture {

	public static void main(String[] args) {
		JavaSparkContext javaSparkContext = JavaRDDSparkContextMain.javaSparkContext("SparkMLClusteringGaussianMixture", "local[*]");
		String dataPath = "data/mllib/gmm_data.txt";
		JavaRDD<String> fileData = javaSparkContext.textFile(dataPath);
		JavaRDD<Vector> parsedData = fileData.map(line -> {
			String[] stringArray = line.trim().split(" ");
			double[] valueArray = new double[stringArray.length];
			for(int size = 0; size < stringArray.length; size++) {
				valueArray[size] = Double.valueOf(stringArray[size]);
			}
			Vector vector = Vectors.dense(valueArray);
			return vector;
		});
		long fileDataCount = fileData.count();
		long parsedDataCount = parsedData.count();
		System.out.println("-----fileDataCount: " + fileDataCount + ",   parsedDataCount: " + parsedDataCount);
		parsedData.cache();
		
		GaussianMixtureModel gaussianMixtureModel = new GaussianMixture().setK(2).run(parsedData.rdd());
		System.out.println("-----gaussianMixtureModel: " + gaussianMixtureModel);
		System.out.println("-----gaussianMixtureModel k: " + gaussianMixtureModel.k() );
		
		for(int k = 0; k < gaussianMixtureModel.k(); k++) {
			System.out.println("-----weights: " + gaussianMixtureModel.weights()[k] 
					+ ",    mu: " + gaussianMixtureModel.gaussians()[k] 
					+ ",    sigma: " + gaussianMixtureModel.gaussians()[k].sigma());
		}
		
		

	}

}
