package com.highill.practice.spark.mllib.rdd.ensembles;

import java.util.HashMap;
import java.util.Map;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.tree.GradientBoostedTrees;
import org.apache.spark.mllib.tree.configuration.BoostingStrategy;
import org.apache.spark.mllib.tree.model.GradientBoostedTreesModel;
import org.apache.spark.mllib.util.MLUtils;

import scala.Tuple2;

import com.highill.practice.spark.JavaRDDSparkContextMain;

public class SparkMLGradientBoostedTreesRegression {

	public static void main(String[] args) {
		JavaSparkContext javaSparkContext = JavaRDDSparkContextMain.javaSparkContext("SparkMLGradientBoostedTreesRegression", "local[*]");
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
				+ ",    trainingDataCount: " +trainingDataCount 
				+ ",    testDataCount: " + testDataCount);
		
		BoostingStrategy boostingStrategy = BoostingStrategy.defaultParams("Regression");
		boostingStrategy.setNumIterations(3);
		boostingStrategy.getTreeStrategy().setMaxDepth(5);
		Map<Integer, Integer> categoricalFeaturesMap = new HashMap<Integer, Integer>();
		boostingStrategy.getTreeStrategy().setCategoricalFeaturesInfo(categoricalFeaturesMap);
		final GradientBoostedTreesModel gradientBoostedTreesModel = GradientBoostedTrees.train(trainingData, boostingStrategy);
		System.out.println("-----gradientBoostedTreesModel debug first: \n" + gradientBoostedTreesModel.toDebugString());
		System.out.println("-----categoricalFeaturesMap size: " + categoricalFeaturesMap.size());
		
		JavaPairRDD<Double, Double> predictionAndLabel = testData.mapToPair(labeledPoint -> {
			double prediction = gradientBoostedTreesModel.predict(labeledPoint.features());
			System.out.println("-----labeledPoint features size: " + labeledPoint.features().size() + ",  label: " + labeledPoint.label());
			Tuple2<Double, Double> tuple = new Tuple2<Double, Double>(prediction, labeledPoint.label());
			return tuple;
		});
		Double testError = predictionAndLabel.map(tuple -> {
			Double diff = tuple._1() - tuple._2();
			return diff;
		}).reduce((t1, t2) -> (t1 + t2));
		System.out.println("-----testError: " + testError);
		System.out.println("-----gradientBoostedTreesModel debug finish: \n" + gradientBoostedTreesModel.toDebugString());

	}

}
