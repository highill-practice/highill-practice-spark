package com.highill.practice.spark.mllib.rdd;

import java.io.File;
import java.util.Arrays;
import java.util.List;






import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.Matrices;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.random.RandomRDDs;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.stat.KernelDensity;
import org.apache.spark.mllib.stat.MultivariateStatisticalSummary;
import org.apache.spark.mllib.stat.Statistics;
import org.apache.spark.mllib.stat.test.BinarySample;
import org.apache.spark.mllib.stat.test.ChiSqTestResult;
import org.apache.spark.mllib.stat.test.KolmogorovSmirnovTestResult;
import org.apache.spark.mllib.stat.test.StreamingTest;
import org.apache.spark.mllib.stat.test.StreamingTestResult;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.spark_project.guava.base.Function;
import org.spark_project.guava.collect.ImmutableMap;

import scala.Tuple2;

import com.highill.practice.spark.JavaRDDSparkContextMain;

/**
 * http://spark.apache.org/docs/2.1.0/mllib-statistics.html
 */
public class SparkMLRDDBasicStatistics {

	public static void main(String[] args) {
		JavaSparkContext javaSparkContext = JavaRDDSparkContextMain.javaSparkContext("Spark ML Basic Statistics ", "local[*]");
		
		// Summary statistics
		JavaRDD<Vector> vectorRDD = javaSparkContext.parallelize( Arrays.asList(
				Vectors.dense(1.0, 10.0, 100.0, 500.0), Vectors.dense(2.0, 20.0, 200.0, 1100.0),
				Vectors.dense(3.0, 30.0, 300.0, 0.0)
						));
		System.out.println("-----vectorRDD: " + vectorRDD);
		System.out.println("-----vector collect: " + vectorRDD.collect());
		MultivariateStatisticalSummary summary = Statistics.colStats(vectorRDD.rdd());
		System.out.println("----- summary: " + summary);
		System.out.println("----- summary mean (平均值): " + summary.mean());
		System.out.println("----- summary variance (方差): " + summary.variance());
		System.out.println("----- summary numNonzeros (非零数个数): " + summary.numNonzeros());
		
		
		// Correlations 相关性  
		JavaDoubleRDD doubleX = javaSparkContext.parallelizeDoubles(
				Arrays.asList(1.0, 2.0, 3.0, 4.0, 5.0));
		JavaDoubleRDD doubleY = javaSparkContext.parallelizeDoubles(
				Arrays.asList(11.0, 22.0, 33.0, 44.0, 105.00));
		Double correlation = Statistics.corr(doubleX.srdd(), doubleY.srdd(), "pearson");
		Double correlationDefault = Statistics.corr(doubleX.srdd(), doubleY.srdd());
		System.out.println("----- correlation pearson: " + correlation);
		System.out.println("----- correlation default: " + correlationDefault);
		
		Matrix correlationMatrix = Statistics.corr(vectorRDD.rdd());
		System.out.println("----- correlationMatrix: \n" + correlationMatrix + "\n\n");
		
		
		// Stratified sampling 分层取样  
		List<Tuple2<Integer, Character>> tupleList = Arrays.asList(
				new Tuple2<Integer, Character>(1, 'a'),
				new Tuple2<>(1, 'b'),
				new Tuple2<>(2, 'c'),
				new Tuple2<>(2, 'd'),
				new Tuple2<>(2, 'e'),
				new Tuple2<>(3, 'f')
				);
		JavaPairRDD<Integer, Character> tuplePairRDD = javaSparkContext.parallelizePairs(tupleList);
		System.out.println("----- tuplePairRDD: " + tuplePairRDD);
		System.out.println("----- tuplePairRDD collect: " + tuplePairRDD.collect());
		ImmutableMap<Integer, Double> immutableFraction = ImmutableMap.of(1,  0.1, 2, 0.6, 3, 0.3);
		JavaPairRDD<Integer, Character> sampleByKeyPairRDD = tuplePairRDD.sampleByKey(false, immutableFraction);
		JavaPairRDD<Integer, Character> sampleByKeyExactPairRDD = tuplePairRDD.sampleByKeyExact(false, immutableFraction);
		System.out.println("----- sampleByKeyPairRDD: " + sampleByKeyPairRDD.collect());
		System.out.println("----- sampleByKeyExactPairRDD: " + sampleByKeyExactPairRDD.collect());
		
		
		
		
		
		// Hypothesis testing 假设检验  
		// chiSq 卡方, 卡平方值  
		Vector hypothesisVector = Vectors.dense(0.1, 0.15, 0.2, 0.3, 0.25, 0.5);
		ChiSqTestResult goodnessOfFitTestResult = Statistics.chiSqTest(hypothesisVector);
		System.out.println("----- goodnessOfFitTestResult: \n" + goodnessOfFitTestResult);
		
		Matrix hypothesisMatrix = Matrices.dense(3, 2, new double[]{1.0, 3.0, 5.0, 2.0, 4.0, 6.0});
		ChiSqTestResult independenceTestResult = Statistics.chiSqTest(hypothesisMatrix);
		System.out.println("----- independenceTestResult: \n" + independenceTestResult);
		
		JavaRDD<LabeledPoint> labeledRDD = javaSparkContext.parallelize(
				Arrays.asList(
						new LabeledPoint(1.0, Vectors.dense(1.0, 0.0, 3.0)),
						new LabeledPoint(1.0, Vectors.dense(1.0, 2.0, 0.0)),
						new LabeledPoint(-1.0, Vectors.dense(-0.1, 0.0, -0.5))
						));
		ChiSqTestResult[] featureTestResults = Statistics.chiSqTest(labeledRDD.rdd());
		int columnSize = 1;
		for(ChiSqTestResult result : featureTestResults) {
			System.out.println("----- columnSize = " + columnSize + ",  result is: \n" + result);
			columnSize++;
		}
		
		// kolmogorovSmirnov 柯斯二式统计
		JavaDoubleRDD hypothesisDoubleRDD = javaSparkContext.parallelizeDoubles(Arrays.asList(0.1, 0.15, 0.2, 0.3, 0.25));
		KolmogorovSmirnovTestResult doubleRDDResult = Statistics.kolmogorovSmirnovTest(hypothesisDoubleRDD, "norm", 0.1, 1.0);
		System.out.println("-----doubleRDDResult: \n" + doubleRDDResult);
		
		
		// TODO 
		// Streaming Significance Testing 流的显著性测试  
		JavaStreamingContext javaStreamingContext = new JavaStreamingContext(javaSparkContext, new Duration(1000));
		String dataDir = "data/streaming_significance.txt";
		// File file = new File(dataDir);
		JavaDStream<BinarySample> fileStream = javaStreamingContext.textFileStream(dataDir).map(line -> 
		{
			String[] stringArray = line.split(",");
			boolean label = Boolean.parseBoolean(stringArray[0]);
			double doubleValue = Double.parseDouble(stringArray[1]);
			return new BinarySample(label, doubleValue);
		});
		StreamingTest streamingTest = new StreamingTest()
		  .setPeacePeriod(0).setWindowSize(0).setTestMethod("welch");
		JavaDStream<StreamingTestResult> fileStreamOut = streamingTest.registerStream(fileStream);
		System.out.println("-----fileStreamOut: " + fileStreamOut);
		fileStreamOut.print();
		//javaStreamingContext.close();
		
		
		// Random data generation 随机数据生成  
		JavaDoubleRDD randomDoubleRDD = RandomRDDs.normalJavaRDD(javaSparkContext, 1000L, 10);
		JavaDoubleRDD randomDoubleRDD2 = randomDoubleRDD.mapToDouble(d -> (1.0 + 2.0 * d));
		System.out.println("-----Random data randomDoubleRDD: " + randomDoubleRDD.take(5) + " ,  randomDoubleRDD2: " + randomDoubleRDD2.take(5));
		
		
		
		// Kernel density estimation 核密度估计  
		JavaRDD<Double> kernelDoubleRDD = javaSparkContext.parallelize(
				Arrays.asList(1.0, 1.0, 1.0, 2.0, 3.0, 5.0, 5.0, 6.0, 7.0, 8.0, 9.0, 9.0));
		KernelDensity kernelDensity = new KernelDensity().setSample(kernelDoubleRDD).setBandwidth(3.0);
		double[] densityArray = kernelDensity.estimate(new double[]{-1.0, 2.0, 5.0});
		System.out.println("-----Kernel densityArray: " + Arrays.toString(densityArray));
		
		

	}

}
