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

public class SparkMLDecisionTreeClassification {

	public static void main(String[] args) {
		JavaSparkContext javaSparkContext = JavaRDDSparkContextMain.javaSparkContext("SparkMLDecisionTreeClassification", "local[*]");
		SparkContext sparkContext = javaSparkContext.sc();
		String dataPath = "data/mllib/sample_libsvm_data.txt";
		JavaRDD<LabeledPoint> labeledData = MLUtils.loadLibSVMFile(sparkContext, dataPath).toJavaRDD();
		System.out.println("-----labeledPoint count: " + labeledData.count());
		
		// Split the data into training and test (30% held out for testing)
		JavaRDD<LabeledPoint>[] splitArray = labeledData.randomSplit(new double[]{0.7, 0.3});
		JavaRDD<LabeledPoint> trainingData = splitArray[0];
		JavaRDD<LabeledPoint> testData = splitArray[1];
		System.out.println("-----trainingData count: " + trainingData.count());
		System.out.println("-----testData count: " + testData.count());
		
		Integer numClasses = 2;
		Map<Integer, Integer> categoricalFeaturesMap = new HashMap<Integer, Integer>();
		String impurity = "gini";
		Integer maxDepth = 5;
		Integer maxBins = 32;
		
		// Train a DecisionTree model for classification
		final DecisionTreeModel decisionTreeModel = DecisionTree.trainClassifier(trainingData, numClasses, categoricalFeaturesMap, impurity, maxDepth, maxBins);
		System.out.println("-----decisionTreeModel: " + decisionTreeModel);
		
		JavaPairRDD<Double, Double> predictionAndLabel = testData.mapToPair(labeledPoint -> {
			double prediction = decisionTreeModel.predict(labeledPoint.features());
			System.out.println("-----labeledPoint features size: " + labeledPoint.features().size() + ",  labeledPoint label: " + labeledPoint.label());
			Tuple2<Double, Double> tuple = new Tuple2<Double, Double>(prediction, labeledPoint.label());
			return tuple;
		});
		System.out.println("----- predictionAndLabel: " + predictionAndLabel);
		long errorCount = predictionAndLabel.filter(labeledPoint -> {
			boolean result = !labeledPoint._1().equals(labeledPoint._2());
			return result;
		}).count();
		double errorDouble = 1.0 * errorCount;
		System.out.println("-----test error count: " + errorCount + ",  or " + errorDouble);
		System.out.println("-----dedision tree classificaition model: \n" + decisionTreeModel.toDebugString());

	}

}
