package com.highill.practice.spark.mllib.rdd.naive.bayes;

import java.util.Arrays;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.classification.NaiveBayes;
import org.apache.spark.mllib.classification.NaiveBayesModel;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.util.MLUtils;

import scala.Tuple2;

import com.highill.practice.spark.JavaRDDSparkContextMain;

public class SparkMLNaiveBayes {

	public static void main(String[] args) {
		JavaSparkContext javaSparkContext = JavaRDDSparkContextMain.javaSparkContext("SparkMLNaiveBayes", "local[*]");
		SparkContext sparkContext = javaSparkContext.sc();
		
		String dataPath = "data/mllib/sample_libsvm_data.txt";
		JavaRDD<LabeledPoint> labeledData = MLUtils.loadLibSVMFile(sparkContext, dataPath).toJavaRDD();
		JavaRDD<LabeledPoint>[] splitArray = labeledData.randomSplit(new double[]{0.6, 0.4});
		JavaRDD<LabeledPoint> trainingData = splitArray[0];
		JavaRDD<LabeledPoint> testData = splitArray[1];
		long labeledDataCount = labeledData.count();
		long trainingDataCount = trainingData.count();
		long testDataCount = testData.count();
		System.out.println("-----labeledDataCount: " + labeledDataCount 
				+ ",    trainingDataCount: " + trainingDataCount 
				+ ",    testDataCount: " + testDataCount);
		
		final NaiveBayesModel naiveBayesModel = NaiveBayes.train(trainingData.rdd(), 1.0);
		System.out.println("-----naiveBayesModel debug first: " + naiveBayesModel);
		
		JavaPairRDD<Double, Double> predictionAndLabel = testData.mapToPair(labeledPoint -> {
			double prediction = naiveBayesModel.predict(labeledPoint.features());
			System.out.println("-----labeledPoint features size: " + labeledPoint.features().size() 
					+ ",    label: " + labeledPoint.label());
			Tuple2<Double, Double> tuple = new Tuple2<Double, Double>(prediction, labeledPoint.label());
			return tuple;
		});
		double accuracy = predictionAndLabel.filter(tuple -> {
			boolean result = tuple._1().equals(tuple._2());
			return result;
		}).count();
		System.out.println("-----accuracy: " + accuracy);
		String formatVersion = naiveBayesModel.formatVersion();
		double[] pi = naiveBayesModel.pi();
		String modelType = naiveBayesModel.modelType();
		System.out.println("-----naiveBayesModel formatVersion:" + formatVersion 
				+ ",    pi:" + Arrays.toString(pi) 
				+ ",    modelType: " + modelType);
		

	}

}
