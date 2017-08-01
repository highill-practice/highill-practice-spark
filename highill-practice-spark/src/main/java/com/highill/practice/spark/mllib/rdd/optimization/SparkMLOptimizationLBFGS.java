package com.highill.practice.spark.mllib.rdd.optimization;

import java.util.Arrays;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.classification.LogisticRegressionModel;
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.optimization.LBFGS;
import org.apache.spark.mllib.optimization.LogisticGradient;
import org.apache.spark.mllib.optimization.SquaredL2Updater;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.util.MLUtils;

import scala.Tuple2;

import com.highill.practice.spark.JavaRDDSparkContextMain;

/**
 * 
 * http://spark.apache.org/docs/2.1.0/mllib-optimization.html#l-bfgs
 *
 */
public class SparkMLOptimizationLBFGS {

	public static void main(String[] args) {
		JavaSparkContext javaSparkContext = JavaRDDSparkContextMain.javaSparkContext("SparkMLOptimizationLBFGS", "local[*]");
		SparkContext sparkContext = javaSparkContext.sc();
		
		String dataPath = "data/mllib/sample_libsvm_data.txt";
		JavaRDD<LabeledPoint> labeledData = MLUtils.loadLibSVMFile(sparkContext, dataPath).toJavaRDD();
		int numFeatures = labeledData.take(1).get(0).features().size();
		System.out.println("-----labeledData count: " + labeledData.count() + ",    numFeatures: " + numFeatures);
		
		JavaRDD<LabeledPoint> trainingData = labeledData.sample(false,0.6, 11L);
		JavaRDD<LabeledPoint> testData = labeledData.subtract(trainingData);
		long trainingDataCount = trainingData.count();
		long testDataCount = testData.count();
		System.out.println("-----trainingDataCount: " + trainingDataCount + ",    testDataCount: " + testDataCount);
		
		JavaRDD<Tuple2<Object, Vector>> trainingTuple = labeledData.map(labeledPoint -> {
			Vector vector = MLUtils.appendBias(labeledPoint.features());
			Tuple2<Object, Vector> tuple = new Tuple2<Object, Vector>(labeledPoint.label(), vector);
			return tuple;
		});
		System.out.println("-----trainingTuple count: " + trainingTuple.count());
		trainingTuple.cache();
		
		int numCorrections = 10;
		double convergenceTol = 1e-4;
		int maxNumIterations = 20;
		double regParam = 0.1;
		Vector initialWeightsWithIntercept = Vectors.dense(new double[numFeatures + 1]);
		Tuple2<Vector, double[]> result = LBFGS.runLBFGS(trainingTuple.rdd(), new LogisticGradient(), new SquaredL2Updater(), 
				numCorrections, convergenceTol, maxNumIterations, regParam, initialWeightsWithIntercept);
		double[] loss = result._2();
		Vector weightsWithIntercept = result._1();
		System.out.println("-----result productIterator size: " + result.productIterator().size());
		System.out.println("-----result loss: " + Arrays.toString(loss));
		System.out.println("-----result vecotr: "  + weightsWithIntercept.size());
		
		final LogisticRegressionModel logisticRegressionModel = new LogisticRegressionModel(
				Vectors.dense(Arrays.copyOf(weightsWithIntercept.toArray(), weightsWithIntercept.size() - 1)),
				(weightsWithIntercept.toArray())[weightsWithIntercept.size() - 1]
				);
		System.out.println("-----logisticRegressionModel: " + logisticRegressionModel);
		System.out.println("-----logisticRegressionModel numClasses: " + logisticRegressionModel.numClasses() 
				+ ",    numFeatures: " + logisticRegressionModel.numFeatures());
		logisticRegressionModel.clearThreshold();
		
		
		JavaRDD<Tuple2<Object, Object>> scoreAndLabels = testData.map(labeledPoint -> {
			Double score = logisticRegressionModel.predict(labeledPoint.features());
			Tuple2<Object, Object> tuple = new Tuple2<Object, Object>(score, labeledPoint.label());
			return tuple;
		});
		System.out.println("-----scoreAndLabeld count: " + scoreAndLabels.count());
		
		BinaryClassificationMetrics binaryClassificationMetrics = new BinaryClassificationMetrics(scoreAndLabels.rdd());
		double areaUnderROC = binaryClassificationMetrics.areaUnderROC();
		double areaUnderPR = binaryClassificationMetrics.areaUnderPR();
		System.out.println("-----binaryClassificationMetrics: " + binaryClassificationMetrics);
		System.out.println("-----areaUnderROC: " + areaUnderROC + ",  areaUnderPR: " + areaUnderPR);
		System.out.println("-----loss: " + Arrays.toString(loss));
		
		

	}

}
