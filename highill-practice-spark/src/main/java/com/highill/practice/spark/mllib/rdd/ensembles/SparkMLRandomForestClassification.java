package com.highill.practice.spark.mllib.rdd.ensembles;

import java.util.HashMap;

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

public class SparkMLRandomForestClassification {

	public static void main(String[] args) {
		JavaSparkContext javaSparkContext = JavaRDDSparkContextMain.javaSparkContext("SparkMLRandomForestClassification", "local[*]");
		SparkContext sparkContext = javaSparkContext.sc();
		String dataPath = "data/mllib/sample_libsvm_data.txt";
		JavaRDD<LabeledPoint> labeledData = MLUtils.loadLibSVMFile(sparkContext, dataPath).toJavaRDD();
		
		JavaRDD<LabeledPoint>[] splitArray = labeledData.randomSplit(new double[]{0.7, 0.4});
		JavaRDD<LabeledPoint> trainingData = splitArray[0];
		JavaRDD<LabeledPoint> testData = splitArray[1];
		System.out.println("-----labeledData count: " + labeledData.count());
		System.out.println("-----trainingData count: " + trainingData.count());
		System.out.println("-----testData count: " + testData.count());
		
		// Train a RandomForest model
		Integer numClasses = 2;
		HashMap<Integer, Integer> categoricalFeaturesMap = new HashMap<Integer, Integer>();
		Integer numTrees = 3;
		String featureSubsetStrategy = "auto";
		String impurity = "gini";
		Integer maxDepth = 5;
		Integer maxBins = 22;
		Integer seed = 12345;
		final RandomForestModel randomForestModel = RandomForest.trainClassifier(trainingData, numClasses, categoricalFeaturesMap , numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins, seed);
		System.out.println("-----randomForestModel: " + randomForestModel);
		System.out.println("-----randomForestModel debug: " + randomForestModel.toDebugString());
		System.out.println("-----categoricalFeaturesMap size: " + categoricalFeaturesMap.size());
		
		
		JavaPairRDD<Double, Double> predictionAndLabel = testData.mapToPair(labeledPoint -> {
			double prediction = randomForestModel.predict(labeledPoint.features());
			Tuple2<Double, Double> tuple = new Tuple2<Double, Double>(prediction, labeledPoint.label());
			return tuple;
		});
		
		long testError = predictionAndLabel.filter(tuple -> {
			boolean result = !tuple._1().equals(tuple._2());
			return result;
		}).count();
		double testErrorDouble = 1.0 * testError;
		System.out.println("-----testError: " + testError + ",  testErrorDouble: " + testErrorDouble);
		System.out.println("-----randomForestModel debug: \n" + randomForestModel.toDebugString());
		
		
		

	}

}
