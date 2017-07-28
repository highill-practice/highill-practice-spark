package com.highill.practice.spark.mllib.rdd.linear;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.classification.LogisticRegressionModel;
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS;
import org.apache.spark.mllib.evaluation.MulticlassMetrics;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.util.MLUtils;

import scala.Tuple2;

import com.highill.practice.spark.JavaRDDSparkContextMain;

public class SparkMLLinearLogisticRegression {

	public static void main(String[] args) {
		JavaSparkContext javaSparkContext = JavaRDDSparkContextMain.javaSparkContext("SparkMLLogisticRegression", "local[*]");
		SparkContext sparkContext = javaSparkContext.sc();
		String dataPath = "data/mllib/sample_libsvm_data.txt";
		JavaRDD<LabeledPoint> labeledData = MLUtils.loadLibSVMFile(sparkContext, dataPath).toJavaRDD();
		
		JavaRDD<LabeledPoint>[] splitArray = labeledData.randomSplit(new double[]{0.6, 0.4},  11L);
		JavaRDD<LabeledPoint> trainingData = splitArray[0].cache();
		JavaRDD<LabeledPoint> testData = splitArray[1];
		System.out.println("-----trainingData count: " + trainingData.count());
		System.out.println("-----testData count: " + testData.count());
		
		final LogisticRegressionModel logisticRegressionModel = 
				new LogisticRegressionWithLBFGS().setNumClasses(10).run(trainingData.rdd());
		System.out.println("-----logisticRegressionModel: " + logisticRegressionModel);
		
		JavaRDD<Tuple2<Object, Object>> predictionAndLabels = testData.map(labeledPoint -> {
			Double prediction = logisticRegressionModel.predict(labeledPoint.features());
			// labeledPoint.productPrefix()
			System.out.println("-----labeledPoint productPrefix: " + labeledPoint.productPrefix() 
					+ ", labeledPoint features: " + labeledPoint.features().size()
					+ " labeledPoint label: " + labeledPoint.label() 
					+ ",  prediction: " + prediction);
			Tuple2<Object, Object> tuple = new Tuple2<Object, Object>(prediction, labeledPoint.label());
			return tuple;
		});
		System.out.println("-----predictionAndLabels: " + predictionAndLabels.count());
		
		MulticlassMetrics multiclassMetrics = new MulticlassMetrics(predictionAndLabels.rdd());
		double accuracy = multiclassMetrics.accuracy();
		System.out.println("-----multiclassMetrics: " + multiclassMetrics);
		System.out.println("-----accuracy: " +accuracy);
		
		
		// TODO 
		// String savePath = "target/tmp/SparkMLLogisticRegression";
		// logisticRegressionModel.save(sparkContext, savePath);

	}

}
