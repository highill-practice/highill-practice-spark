package com.highill.practice.spark.mllib.rdd.linear;

import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.regression.LinearRegressionModel;
import org.apache.spark.mllib.regression.LinearRegressionWithSGD;

import scala.Tuple2;

import com.highill.practice.spark.JavaRDDSparkContextMain;

public class SparkMLLinearRegressionWithSGD {

	public static void main(String[] args) {
		JavaSparkContext javaSparkContext = JavaRDDSparkContextMain.javaSparkContext("SparkMLLinearRegressionWithSGD", "local[*]");
		// SparkContext sparkContext = javaSparkContext.sc();
		String dataPath = "data/mllib/ridge-data/lpsa.txt";
		JavaRDD<String> dataRDD = javaSparkContext.textFile(dataPath);
		JavaRDD<LabeledPoint> labeledData = dataRDD.map(line -> {
			String[] stringArray = line.split(",");
			String[] featuresString = stringArray[1].split(" ");
			double[] featuresDouble = new double[featuresString.length];
			for(int size = 0; size < featuresString.length; size++) {
				featuresDouble[size] = Double.parseDouble(featuresString[size]);
			}
			LabeledPoint labeledPoint = new LabeledPoint(Double.parseDouble(stringArray[0]), Vectors.dense(featuresDouble));
			return labeledPoint;
		});
		System.out.println("-----labeledData count: " + labeledData.count());
		labeledData.cache();
		
		// Building the model
		int numIterations = 100;
		double stepSize = 0.00000001;
		final LinearRegressionModel linearRegressionModel = 
				LinearRegressionWithSGD.train(JavaRDD.toRDD(labeledData), numIterations, stepSize);
		System.out.println("-----linearRegressionModel: " + linearRegressionModel);
		
		// Evaluate model on training examples and compute training error
		JavaRDD<Tuple2<Double, Double>> valuesAndPrediction = labeledData.map(labeledPoint -> {
			double prediction = linearRegressionModel.predict(labeledPoint.features());
			System.out.println("-----labeledPoint features size: " + labeledPoint.features().size() + ",  labeledPoint label: " + labeledPoint.label());
			Tuple2<Double, Double> tuple = new Tuple2<Double, Double>(prediction, labeledPoint.label());
			return tuple;
		});
		System.out.println("-----valuesAndPrediction count: " + valuesAndPrediction.count());
		
		
		JavaDoubleRDD powRDD = valuesAndPrediction.mapToDouble(pair -> {
			double pow = Math.pow(pair._1() - pair._2(), 2.0);
			return pow;
		});
		System.out.println("-----powRDD count: " + powRDD.count());
		double meanSquaredError = powRDD.mean();
		System.out.println("-----meanSquaredError: " + meanSquaredError);
		
		// TODO save
		

	}

}
