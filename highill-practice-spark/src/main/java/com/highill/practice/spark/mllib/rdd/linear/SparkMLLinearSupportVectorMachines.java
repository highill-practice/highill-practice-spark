package com.highill.practice.spark.mllib.rdd.linear;

import java.io.File;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.classification.SVMModel;
import org.apache.spark.mllib.classification.SVMWithSGD;
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics;
import org.apache.spark.mllib.optimization.L1Updater;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.util.MLUtils;

import scala.Tuple2;

import com.highill.practice.spark.JavaRDDSparkContextMain;
import com.highill.practice.spark.tool.FileTool;

/**
 * 
 * https://github.com/apache/spark/blob/master/examples/src/main/java/org/apache/spark/examples/mllib/JavaSVMWithSGDExample.java
 *
 */
public class SparkMLLinearSupportVectorMachines {

	public static void main(String[] args) {
		String savePath = "target/tmp/SparkMLLinearSVMWithSGDModel";
		FileTool.deletePath(savePath);
		
		
		String dataPath = "data/mllib/sample_libsvm_data.txt";
		
		JavaSparkContext javaSparkContext = JavaRDDSparkContextMain.javaSparkContext("SparkMLLinearSupportVectorMachines", "local[*]");
		JavaRDD<LabeledPoint> labeledData = MLUtils.loadLibSVMFile(javaSparkContext.sc(), dataPath).toJavaRDD();
		System.out.println("-----labeledData count: " + labeledData.count());
		System.out.println("-----labeledData take 1: " + labeledData.take(1));
		
		// Split initial RDD into tow collection, [60% training data, 40% testing data]
		JavaRDD<LabeledPoint> trainingData = labeledData.sample(false, 0.6, 11L);
		System.out.println("-----trainingData count: " + trainingData.count());
		trainingData.cache();
		System.out.println("-----trainingData cache count: " + trainingData.count());
		JavaRDD<LabeledPoint> testData = labeledData.subtract(trainingData);
		System.out.println("-----testData count: " + testData.count());
		
		// Run training
		int numIterations = 100;
		final SVMModel svmTrainingModel = SVMWithSGD.train(trainingData.rdd(), numIterations);
		System.out.println("-----svmTrainingModel: " + svmTrainingModel);
		svmTrainingModel.clearThreshold();
		
		// Compute raw scores on the test set
		JavaRDD<Tuple2<Object, Object>> scoreAndLabels = testData.map(labeledPoint -> {
			Double score = svmTrainingModel.predict(labeledPoint.features());
			Tuple2<Object, Object> tuple = new Tuple2<Object, Object>(score, labeledPoint.label());
			return tuple;
		});
		System.out.println("-----scoreAndLabels count: " + scoreAndLabels.count());
		
		BinaryClassificationMetrics binaryClassificationMetrics = 
				new BinaryClassificationMetrics(JavaRDD.toRDD(scoreAndLabels));
		System.out.println("-----binaryClassificationMetrics " + binaryClassificationMetrics);
		double areaUnderROC = binaryClassificationMetrics.areaUnderROC();
		System.out.println("-----areaUnderROC: " + areaUnderROC);
		
		
		
		// TODO
		// svmTrainingModel.save(javaSparkContext.sc(), savePath);
		// SVMModel reloadSVMModel = SVMModel.load(javaSparkContext.sc(), savePath);
		// System.out.println("-----reloadSVMModel: " + reloadSVMModel);
		
		
		SVMWithSGD svmWithSGD = new SVMWithSGD();
		svmWithSGD.optimizer().setNumIterations(200).setRegParam(0.1).setUpdater(new L1Updater());
		final SVMModel svmModelL1 = svmWithSGD.run(trainingData.rdd());
		System.out.println("-----svmModelL1: " + svmModelL1);
		
		javaSparkContext.sc().stop();

	}

}
