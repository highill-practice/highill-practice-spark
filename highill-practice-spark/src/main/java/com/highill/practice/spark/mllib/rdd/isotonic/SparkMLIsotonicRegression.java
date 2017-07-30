package com.highill.practice.spark.mllib.rdd.isotonic;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.regression.IsotonicRegression;
import org.apache.spark.mllib.regression.IsotonicRegressionModel;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.util.MLUtils;

import scala.Tuple2;
import scala.Tuple3;

import com.highill.practice.spark.JavaRDDSparkContextMain;

/**
 * 
 * http://spark.apache.org/docs/2.1.0/mllib-isotonic-regression.html  
 *
 */
public class SparkMLIsotonicRegression {

	public static void main(String[] args) {
		JavaSparkContext javaSparkContext = JavaRDDSparkContextMain.javaSparkContext("SparkMLIsotonicRegression", "local[*]");
		SparkContext sparkContext = javaSparkContext.sc();
		
		String dataPath = "data/mllib/sample_isotonic_regression_libsvm_data.txt";
		JavaRDD<LabeledPoint> labeledData = MLUtils.loadLibSVMFile(sparkContext, dataPath).toJavaRDD();
		JavaRDD<Tuple3<Double, Double, Double>> parsedData = labeledData.map(labeledPoint -> {
			
			Tuple3<Double, Double, Double> tuple = 
					new Tuple3<Double, Double, Double>(
							new Double(labeledPoint.label()), new Double(labeledPoint.features().apply(0)), 1.0
							);
			return tuple;
		});
		long labeledDataCount = labeledData.count();
		long parsedDataCount = parsedData.count();
		System.out.println("-----labeledDataCount: " + labeledDataCount 
				+ ",    parsedDataCount: " + parsedDataCount);
		
		JavaRDD<Tuple3<Double, Double, Double>>[] splitArray = parsedData.randomSplit(new double[]{0.6, 0.4}, 11L);
		JavaRDD<Tuple3<Double, Double, Double>> trainingData = splitArray[0];
		JavaRDD<Tuple3<Double, Double, Double>> testData = splitArray[1];
		long trainingDataCount = trainingData.count();
		long testDataCount = testData.count();
		System.out.println("----- labeledDataCount: " + labeledDataCount 
				+ ",    trainingDataCount: " + trainingDataCount 
				+ ",    testDataCount: " + testDataCount);
		
		
		final IsotonicRegressionModel isotonicRegressionModel = 
				new IsotonicRegression().setIsotonic(true).run(trainingData);
		System.out.println("-----isotonicRegressionModel: " + isotonicRegressionModel);
		
		JavaPairRDD<Double, Double> predictionAndLabel = testData.mapToPair(tuple3 -> {
			Double predictedLabel = isotonicRegressionModel.predict(tuple3._2());
			Tuple2<Double, Double> tuple2 = new Tuple2<Double, Double>(predictedLabel, tuple3._1());
			return tuple2;
		});
		System.out.println("-----predictionAndLabel count: " + predictionAndLabel.count());
		
		JavaRDD<Object> errorRDD = predictionAndLabel.map(tuple -> {
			Double result = Math.pow(tuple._1() - tuple._2(), 2);
			return result;
		});
		Double meanSquaredError = new JavaDoubleRDD(errorRDD.rdd()).mean();
		System.out.println("-----meanSquaredError: " + meanSquaredError);
		

	}

}
