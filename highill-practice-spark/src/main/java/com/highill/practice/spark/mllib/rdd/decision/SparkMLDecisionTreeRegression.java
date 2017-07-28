package com.highill.practice.spark.mllib.rdd.decision;

import java.util.HashMap;
import java.util.Map;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.tree.DecisionTree;
import org.apache.spark.mllib.tree.model.DecisionTreeModel;
import org.apache.spark.mllib.util.MLUtils;

import scala.Tuple2;

import com.highill.practice.spark.JavaRDDSparkContextMain;

/**
 * 
 * http://spark.apache.org/docs/2.1.0/mllib-decision-tree.html
 *
 */
public class SparkMLDecisionTreeRegression {

	public static void main(String[] args) {
		JavaSparkContext javaSparkContext = JavaRDDSparkContextMain.javaSparkContext("SparkMLDecisionTreeRegression", "local[*]");
		SparkContext sparkContext = javaSparkContext.sc();
		
		String dataPath = "data/mllib/sample_libsvm_data.txt";
		JavaRDD<LabeledPoint> labeledData = MLUtils.loadLibSVMFile(sparkContext, dataPath).toJavaRDD();
		// Split the data into training and test (30% held out for testing)
		JavaRDD<LabeledPoint>[] splitArray = labeledData.randomSplit(new double[]{0.7, 0.3});
		JavaRDD<LabeledPoint> trainingData = splitArray[0];
		JavaRDD<LabeledPoint> testData = splitArray[1];
		System.out.println("-----labeledData count: " + labeledData.count());
		System.out.println("-----trainingData count: " + trainingData.count());
		System.out.println("-----testData count: " + testData.count());
		
		Map<Integer, Integer> categoricalFeaturesMap = new HashMap<Integer, Integer>();
		String impurity = "variance";
		Integer maxDepth = 5;
		Integer maxBins = 32;
		final DecisionTreeModel decisionTreeModel = DecisionTree.trainRegressor(trainingData, categoricalFeaturesMap, impurity, maxDepth, maxBins);
		System.out.println("-----decisionTreeModel: \n" + decisionTreeModel.toDebugString());
		System.out.println("-----categoricalFeaturesMap size: " + categoricalFeaturesMap.size());
		
		JavaPairRDD<Double, Double> predictionAndLabel = testData.mapToPair(labeledPoint -> {
			double prediction = decisionTreeModel.predict(labeledPoint.features());
			Tuple2<Double, Double> tuple = new Tuple2<Double, Double>(prediction, labeledPoint.label());
			return tuple;
		});
		System.out.println("----- predictionAndLabel: " + predictionAndLabel.count());
		
		Double testMeanSquaredError = predictionAndLabel.map(tuple -> {
			Double diff = tuple._1() - tuple._2();
			return diff;
		}).reduce((a, b) -> (a + b));
		System.out.println("-----testMeanSquaredError: " + testMeanSquaredError);
		System.out.println("-----decisionTreeModel: \n" + decisionTreeModel.toDebugString());
		System.out.println("-----decisionTreeModel topNode: \n" + decisionTreeModel.topNode());

	}

}
