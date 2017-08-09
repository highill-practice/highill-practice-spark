package com.highill.practice.spark.demo;

import java.util.HashMap;
import java.util.Map;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.classification.LogisticRegressionModel;
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS;
import org.apache.spark.mllib.classification.SVMModel;
import org.apache.spark.mllib.classification.SVMWithSGD;
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics;
import org.apache.spark.mllib.evaluation.MulticlassMetrics;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.tree.DecisionTree;
import org.apache.spark.mllib.tree.GradientBoostedTrees;
import org.apache.spark.mllib.tree.RandomForest;
import org.apache.spark.mllib.tree.configuration.BoostingStrategy;
import org.apache.spark.mllib.tree.model.DecisionTreeModel;
import org.apache.spark.mllib.tree.model.GradientBoostedTreesModel;
import org.apache.spark.mllib.tree.model.RandomForestModel;
import org.apache.spark.mllib.util.MLUtils;

import scala.Tuple2;

import com.highill.practice.spark.JavaRDDSparkContextMain;

public class SaprkDemoMain {

	public static void main(String[] args) {
		JavaSparkContext javaSparkContext = JavaRDDSparkContextMain.javaSparkContext("SparkMLClassificationAndRegression", "local[*]");
		SparkContext sparkContext = javaSparkContext.sc();

		// 2 classes
		String classes2File = "demo/data/libsvm/libsvm_random/libsvm_2_classes.txt";
		JavaRDD<LabeledPoint> labeledClasses2Data = MLUtils.loadLibSVMFile(sparkContext, classes2File).toJavaRDD();
		long labeledClasses2DataCount = labeledClasses2Data.count();
		System.out.println("-----labeledClasses2Data count: " + labeledClasses2DataCount);
		JavaRDD<LabeledPoint>[] classes2DataArray = labeledClasses2Data.randomSplit(new double[]{0.6, 0.4});
		JavaRDD<LabeledPoint> classes2TrainingData = classes2DataArray[0];
		JavaRDD<LabeledPoint> classes2TestData = classes2DataArray[1];
		long classes2TrainingDataCount = classes2TrainingData.count();
		long classes2TestDataCount = classes2TestData.count();
		System.out.println("-----labeledClasses2DataCount: " + labeledClasses2DataCount 
				+ ",    classes2TrainingDataCount: " + classes2TrainingDataCount 
				+ ",    classes2TestDataCount: " + classes2TestDataCount);
		
		// LogisticRegression
		LogisticRegressionWithLBFGS logisticRegressionWithLBFGS = new LogisticRegressionWithLBFGS();
		logisticRegressionWithLBFGS.setNumClasses(10);
		final LogisticRegressionModel logisticRegressionModel = logisticRegressionWithLBFGS.run(classes2TrainingData.rdd());
		System.out.println("-----logisticRegressionModel: " + logisticRegressionModel);
		JavaRDD<Tuple2<Object, Object>> logisticRegressionModelPrediction = classes2TestData.map(labeledPoint -> {
			double prediction = logisticRegressionModel.predict(labeledPoint.features());
			Tuple2<Object, Object> tuple2 = new Tuple2<Object, Object>(prediction, labeledPoint.label());
			return tuple2;
		});
		System.out.println("-----logisticRegressionModelPrediction: " + logisticRegressionModelPrediction);
		System.out.println("-----logisticRegressionModelPrediction count: " + logisticRegressionModelPrediction.count());
		MulticlassMetrics logisticRegressionMetrics = new MulticlassMetrics(logisticRegressionModelPrediction.rdd());
		double logisticRegressionAccuracy = logisticRegressionMetrics.accuracy();
		System.out.println("-----logisticRegressionMetrics: " + logisticRegressionMetrics);
		System.out.println("-----logisticRegressionAccuracy: " + logisticRegressionAccuracy);
		
		// SVMWithSGD
		int svmInterations = 100;
		final SVMModel svmModel = SVMWithSGD.train(classes2TrainingData.rdd(), svmInterations);
		System.out.println("-----svmModel: " + svmModel);
		svmModel.clearThreshold();
		JavaRDD<Tuple2<Object, Object>> svmModelPrediction = classes2TestData.map(labeledPoint -> {
			double prediction = svmModel.predict(labeledPoint.features());
			Tuple2<Object, Object> tuple2 = new Tuple2<Object, Object>(prediction, labeledPoint.label());
			return tuple2;
		});
		System.out.println("-----svmModelPrediction: " + svmModelPrediction);
		System.out.println("-----svmModelPrediction count: " + svmModelPrediction.count());
		BinaryClassificationMetrics svmModelMetrics = new BinaryClassificationMetrics(JavaRDD.toRDD(svmModelPrediction));
		// 正例T 反例F 判断正确P 判断错误N 
		// 查准率(prediction): 正确分类的正例数(TP) 占模型预测正例(TP+FP)的比例, 即 Prediction = TP/(TP+FP)
		// 查全率(recall) 正确分类的正例数(TP) 占实际正例(P=TP+FN)的比例, 即 Recall=TPR=TP/(TP+FN), 查全率也叫灵敏度和召回率  
		// 灵敏度(sensitivity) 正确分类的正例数(TP) 占实际正例 (P=TP+FN)的比例, 即 Sensitivity = TPR=TP/(TP+FN)
		// 特指度(specicity) 正确分类的反例数(TN) 占实际反例(N=TN+FP)的比例, Specicity=TNR=TN/(TN+FP) 1-Specicity=FPR=FP/(TN+FP)  
		double areaUnderPR = svmModelMetrics.areaUnderPR();
		double areaUnderROC = svmModelMetrics.areaUnderROC();
		System.out.println("-----svmModel areaUnderPR: " + areaUnderPR + ",    areaUnderROC: " + areaUnderROC);
		
		// decision tree regression
		Map<Integer, Integer> decisionTreeRegressionFeatureMap = new HashMap<Integer, Integer>();
		String decisionTreeRegressionImpurity = "variance";
		Integer maxDepth = 5;
		Integer maxBinaries = 32;
		final DecisionTreeModel decisionTreeRegressionModel = DecisionTree.trainRegressor(classes2TrainingData, decisionTreeRegressionFeatureMap, 
				decisionTreeRegressionImpurity, maxDepth, maxBinaries);
		System.out.println("-----decisionTreeRegressionModel: " + decisionTreeRegressionModel);
		System.out.println("-----decisionTreeRegressionModel debug: " + decisionTreeRegressionModel.toDebugString());
		JavaRDD<Tuple2<Double, Double>> decisionTreeRegressionPrediction = classes2TestData.map(labeledPoint -> {
			double prediction = decisionTreeRegressionModel.predict(labeledPoint.features());
			Tuple2<Double, Double> tuple2 = new Tuple2<Double, Double>(prediction, labeledPoint.label());
			return tuple2;
		});
		System.out.println("-----decisionTreeRegressionPrediction count: " + decisionTreeRegressionPrediction.count());
		Double testMeanSquaredError = decisionTreeRegressionPrediction.map(tuple2 -> {
			Double diff = tuple2._1() - tuple2._2();
			return diff;
		}).reduce((t1, t2) -> (t1 + t2));
		System.out.println("-----decisionTreeRegression testMeanSquaredError: " + testMeanSquaredError);
		
		// decision tree classification
		int numClasses2 = 2;
		Map<Integer, Integer> decisionTreeClassificationFeatureMap = new HashMap<Integer, Integer>();
		String decisionTreeClassificationImpurity = "gini";
		final DecisionTreeModel decisionTreeClassificationModel = DecisionTree.trainClassifier(classes2TrainingData, numClasses2,
				decisionTreeClassificationFeatureMap, decisionTreeClassificationImpurity, maxDepth, maxBinaries);
		System.out.println("-----decisionTreeClassificationModel: " + decisionTreeClassificationModel);
		System.out.println("-----decisionTreeClassificationModel debug: " + decisionTreeClassificationModel.toDebugString());
		JavaRDD<Tuple2<Double, Double>> decisionTreeClassificationPrediction = classes2TestData.map(labeledPoint -> {
			double prediction = decisionTreeClassificationModel.predict(labeledPoint.features());
			Tuple2<Double, Double> tuple2 = new Tuple2<Double, Double>(prediction, labeledPoint.label());
			return tuple2;
		});
		System.out.println("-----decisionTreeClassificationPrediction count: " + decisionTreeClassificationPrediction.count());
		long errorCount = decisionTreeClassificationPrediction.map(tuple2 -> {
			boolean result = !tuple2._1().equals(tuple2._2());
			return result;
		}).count();
		System.out.println("-----decisionTreeClassificationPrediction errorCount: " + errorCount);
		
		// random forest regression
		Map<Integer, Integer> randomForestRegressionFeatureMap = new HashMap<Integer, Integer>();
		int randomForestTreeNumber = 3;
		String randomForestRegressionFeatureSubsetStrategy = "auto";
		String randomForestRegressionImpurity = "variance";
		Integer randomForestSeed = 12345;
		final RandomForestModel randomForestRegressionModel = RandomForest.trainRegressor(classes2TrainingData, 
				randomForestRegressionFeatureMap, randomForestTreeNumber, 
				randomForestRegressionFeatureSubsetStrategy, randomForestRegressionImpurity, 
				maxDepth, maxBinaries, randomForestSeed);
		System.out.println("----- randomForestRegressionModel: " + randomForestRegressionModel);
		System.out.println("----- randomForestRegressionModel debug: " + randomForestRegressionModel.toDebugString());
		JavaRDD<Tuple2<Double, Double>> randomForestRegressionPrediction = classes2TestData.map(labeledPoint -> {
			double prediction = randomForestRegressionModel.predict(labeledPoint.features());
			Tuple2<Double, Double> tuple2 = new Tuple2<Double, Double>(prediction, labeledPoint.label());
			return tuple2;
		});
		System.out.println("----- randomForestRegressionPrediction: " + randomForestRegressionPrediction);
		Double randomForestRegressionMeanSquaredError = randomForestRegressionPrediction.map(tuple2 -> {
			Double diff = tuple2._1() - tuple2._2();
			return diff;
		}).reduce((t1, t2) -> (t1 + t2));
		System.out.println("----- randomForestRegressionMeanSquaredError: " + randomForestRegressionMeanSquaredError);
		
		// random forest classification  
		Map<Integer, Integer> randomForestClassificationFeatureMap = new HashMap<Integer, Integer>();
		String randomForestClassificationFeatureSubsetStrategy = "auto";
		String randomForestClassificationImpurity = "gini";
		final RandomForestModel randomForestClassificationModel = RandomForest.trainClassifier(classes2TrainingData, 
				numClasses2, randomForestClassificationFeatureMap, randomForestTreeNumber,
				randomForestClassificationFeatureSubsetStrategy, randomForestClassificationImpurity, 
				maxDepth, maxBinaries, randomForestSeed);
		System.out.println("-----randomForestClassificationModel: " + randomForestClassificationModel);
		System.out.println("-----randomForestClassificationModel debug: " + randomForestClassificationModel.toDebugString());
		JavaPairRDD<Double, Double> randomForestClassificationPrediction = classes2TestData.mapToPair(labeledPoint -> {
			double prediction = randomForestClassificationModel.predict(labeledPoint.features());
			Tuple2<Double, Double> tuple2 = new Tuple2<Double, Double>(prediction, labeledPoint.label());
			return tuple2;
		});
		System.out.println("-----randomForestClassificationPrediction count: " + randomForestClassificationPrediction.count());
		long randomForestClassificationError = randomForestClassificationPrediction.filter(tuple2 -> {
			boolean result = !tuple2._1().equals(tuple2._2());
			return result;
		}).count();
		System.out.println("-----randomForestClassificationError: " + randomForestClassificationError);
		
		
		// gradient boosted regression
		BoostingStrategy boostingStrategyRegression = BoostingStrategy.defaultParams("Regression");
		boostingStrategyRegression.setNumIterations(3);
		boostingStrategyRegression.getTreeStrategy().setMaxDepth(maxDepth);
		boostingStrategyRegression.getTreeStrategy().setMaxBins(maxBinaries);
		Map<Integer, Integer> boostingRegressionFeatureMap = new HashMap<Integer, Integer>();
		boostingStrategyRegression.getTreeStrategy().setCategoricalFeaturesInfo(boostingRegressionFeatureMap);
		final GradientBoostedTreesModel gradientBoostedTreesRegressionModel = GradientBoostedTrees.train(classes2TrainingData, boostingStrategyRegression);
		System.out.println("-----gradientBoostedTreesRegressionModel: " + gradientBoostedTreesRegressionModel);
		System.out.println("-----gradientBoostedTreesRegressionModel debug: " + gradientBoostedTreesRegressionModel.toDebugString());
		JavaPairRDD<Double, Double> gradientBoostedTreesRegressionPrediction = classes2TestData.mapToPair(labeledPoint -> {
			double prediction = gradientBoostedTreesRegressionModel.predict(labeledPoint.features());
			Tuple2<Double, Double> tuple2 = new Tuple2<Double, Double>(prediction, labeledPoint.label());
			return tuple2;
		});
		System.out.println("-----gradientBoostedTreesRegressionPrediction count: " + gradientBoostedTreesRegressionPrediction.count());
		Double gradientBoostedTreesMeanSquaredError = gradientBoostedTreesRegressionPrediction.map(tuple2 -> {
			Double diff = tuple2._1() - tuple2._2();
			return diff;
		}).reduce((t1, t2) -> (t1 + t2));
		System.out.println("-----gradientBoostedTreesMeanSquaredError: " + gradientBoostedTreesMeanSquaredError);
		
		// gradient boosted classification
		BoostingStrategy boostingStrategyClassification = BoostingStrategy.defaultParams("Classification");
		boostingStrategyClassification.setNumIterations(3);
		boostingStrategyClassification.getTreeStrategy().setNumClasses(2);
		boostingStrategyClassification.getTreeStrategy().setMaxDepth(maxDepth);
		boostingStrategyClassification.getTreeStrategy().setMaxBins(maxBinaries);
		Map<Integer, Integer> boostingClassificationFeatureMap = new HashMap<Integer, Integer>();
		boostingStrategyClassification.getTreeStrategy().setCategoricalFeaturesInfo(boostingClassificationFeatureMap);
		final GradientBoostedTreesModel gradientBoostedTreesClassificationModel = GradientBoostedTrees.train(classes2TrainingData, boostingStrategyClassification);
		System.out.println("----- gradientBoostedTreesClassificationModel: " + gradientBoostedTreesClassificationModel);
		System.out.println("----- gradientBoostedTreesClassificationModel debug: " + gradientBoostedTreesClassificationModel.toDebugString());
		JavaPairRDD<Double, Double> gradientBoostedTreesClassificationPrediction = classes2TestData.mapToPair(labeledPoint -> {
			double prediction = gradientBoostedTreesClassificationModel.predict(labeledPoint.features());
			Tuple2<Double, Double> tuple2 = new Tuple2<Double, Double>(prediction, labeledPoint.label());
			return tuple2;
		});
		System.out.println("-----gradientBoostedTreesClassificationPrediction count: " + gradientBoostedTreesClassificationPrediction.count());
		long gradientBoostedTreesClassificationError = gradientBoostedTreesClassificationPrediction.filter(tuple2 -> {
			boolean result = !tuple2._1().equals(tuple2._2());
			return result;
		}).count();
		System.out.println("-----gradientBoostedTreesClassificationError: " + gradientBoostedTreesClassificationError);
		
		
		
		// 4 classes
		String classes4File = "demo/data/libsvm/libsvm_random/libsvm_4_classes.txt";
		JavaRDD<LabeledPoint> labeledClasses4Data = MLUtils.loadLibSVMFile(sparkContext, classes4File).toJavaRDD();
		System.out.println("-----labeledClsses4Data count: " + labeledClasses4Data.count());
		

	}

}
