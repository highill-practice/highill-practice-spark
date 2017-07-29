package com.highill.practice.spark.mllib.rdd.ensembles;

import java.util.HashMap;
import java.util.Map;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.tree.RandomForest;
import org.apache.spark.mllib.tree.model.RandomForestModel;
import org.apache.spark.mllib.util.MLUtils;

import scala.Tuple2;

import com.highill.practice.spark.JavaRDDSparkContextMain;

public class SparkMLRandomForestRegression {

	public static void main(String[] args) {
		JavaSparkContext javaSparkContext = JavaRDDSparkContextMain.javaSparkContext("SparkMLRandomForestRegression", "local[*]");
		SparkContext sparkContext = javaSparkContext.sc();
		String dataPath = "data/mllib/sample_libsvm_data.txt";
		JavaRDD<LabeledPoint> labeledData = MLUtils.loadLibSVMFile(sparkContext, dataPath).toJavaRDD();
		
		JavaRDD<LabeledPoint>[] splitArray = labeledData.randomSplit(new double[]{0.7, 0.3});
		JavaRDD<LabeledPoint> trainingData = splitArray[0];
		JavaRDD<LabeledPoint> testData = splitArray[1];
		long labeledDataCount = labeledData.count();
		long trainingDataCount = trainingData.count();
		long testDataCount = testData.count();
		System.out.println("-----labeledDataCount: " + labeledDataCount 
				+ ",  trainingDataCount: " + trainingDataCount 
				+ ",  testDataCount: " + testDataCount);
		
		
		Map<Integer, Integer> categoricalFeaturesMap = new HashMap<Integer, Integer>();
		Integer numTrees = 3;
		String featureSubsetStrategy = "auto";
		String impurity = "variance";
		Integer maxDepth = 4;
		Integer maxBins = 32;
		Integer seed = 12345;
		final RandomForestModel randomForestModel = 
				RandomForest.trainRegressor(trainingData, categoricalFeaturesMap, numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins, seed);
		System.out.println("-----randomForestModel: " + randomForestModel);
		System.out.println("-----randomForestModel debug: \n" + randomForestModel.toDebugString());
		System.out.println("-----categoricalFeaturesMap size: " + categoricalFeaturesMap.size());
		
		
		JavaPairRDD<Double, Double> predictionAndLabel = testData.mapToPair(labeledPoint -> {
			double prediction = randomForestModel.predict(labeledPoint.features());
			Tuple2<Double, Double> tuple = new Tuple2<Double, Double>(prediction, labeledPoint.label());
			return tuple;
		});
		System.out.println("-----predictionAndLabel count: " + predictionAndLabel.count());
		
		Double testMeanSquaredError = predictionAndLabel.map(tuple -> {
			Double diff = tuple._1() - tuple._2();
			return diff;
		}).reduce((t1, t2) -> (t1 + t2));
		System.out.println("-----testMeanSquaredError: " + testMeanSquaredError);
		System.out.println("-----randomForestModel debug: \n" + randomForestModel.toDebugString());

		
		
	}

}
