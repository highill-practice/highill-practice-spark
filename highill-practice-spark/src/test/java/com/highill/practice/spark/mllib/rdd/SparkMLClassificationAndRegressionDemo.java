package com.highill.practice.spark.mllib.rdd;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.util.MLUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.BlockJUnit4ClassRunner;

import com.highill.practice.spark.JavaRDDSparkContextMain;

@RunWith(BlockJUnit4ClassRunner.class)
public class SparkMLClassificationAndRegressionDemo {
	
	@Test
	public void classificationAndRegression() {
		JavaSparkContext javaSparkContext = JavaRDDSparkContextMain.javaSparkContext("SparkMLClassificationAndRegression", "local[*]");
		SparkContext sparkContext = javaSparkContext.sc();
		
		// 文件不存在: ERROR Shell: Failed to locate the winutils binary in the hadoop binary path
		String classes2File = "demo/data/libsvm/libsvm_random/libsvm_2_classes.txt";
		JavaRDD<LabeledPoint> labeledClasses2Data = MLUtils.loadLibSVMFile(sparkContext, classes2File).toJavaRDD();
		System.out.println("-----labeledClasses2Data count: " + labeledClasses2Data.count());
		
	}

}
