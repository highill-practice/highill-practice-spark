package com.highill.practice.spark.demo;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.util.MLUtils;

import com.highill.practice.spark.JavaRDDSparkContextMain;

public class SaprkDemoMain {

	public static void main(String[] args) {
		JavaSparkContext javaSparkContext = JavaRDDSparkContextMain.javaSparkContext("SparkMLClassificationAndRegression", "local[*]");
		SparkContext sparkContext = javaSparkContext.sc();

		// 2 classes
		String classes2File = "demo/data/libsvm/libsvm_random/libsvm_2_classes.txt";
		JavaRDD<LabeledPoint> labeledClasses2Data = MLUtils.loadLibSVMFile(sparkContext, classes2File).toJavaRDD();
		System.out.println("-----labeledClasses2Data count: " + labeledClasses2Data.count());
		
		
		// 4 classes
		String classes4File = "demo/data/libsvm/libsvm_random/libsvm_4_classes.txt";
		JavaRDD<LabeledPoint> labeledClasses4Data = MLUtils.loadLibSVMFile(sparkContext, classes4File).toJavaRDD();
		System.out.println("-----labeledClsses4Data count: " + labeledClasses4Data.count());
		

	}

}
