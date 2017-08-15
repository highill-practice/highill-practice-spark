package com.highill.practice.spark.demo;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.classification.NaiveBayes;
import org.apache.spark.mllib.classification.NaiveBayesModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.util.MLUtils;

import scala.Tuple2;

import com.highill.practice.spark.JavaRDDSparkContextMain;

public class SparkMLTextAnalyzeMain {
	
	private static Map<Character, Double> wordValueMap = new TreeMap<Character, Double>();
	
	static{
		wordValueMap.put('出', Double.valueOf(1001));
		wordValueMap.put('售', Double.valueOf(1002));
		wordValueMap.put('发', Double.valueOf(1003));
		wordValueMap.put('票', Double.valueOf(1004));
		wordValueMap.put('中', Double.valueOf(1005));
		wordValueMap.put('奖', Double.valueOf(1006));
		wordValueMap.put('你', Double.valueOf(1007));
		wordValueMap.put('好', Double.valueOf(1008));
		wordValueMap.put('欢', Double.valueOf(1009));
		wordValueMap.put('迎', Double.valueOf(1010));
		wordValueMap.put('大', Double.valueOf(1011));
		wordValueMap.put('家', Double.valueOf(1012));
		wordValueMap.put('今', Double.valueOf(1013));
		wordValueMap.put('天', Double.valueOf(1014));
		wordValueMap.put('任', Double.valueOf(1015));
		wordValueMap.put('务', Double.valueOf(1016));
		wordValueMap.put('通', Double.valueOf(1017));
		wordValueMap.put('知', Double.valueOf(1018));
		wordValueMap.put('明', Double.valueOf(1019));
		
	}
	
	public static double[] textValueArray(String text) {
		double[] valueArray = null;
		List<Double> valueList = new ArrayList<Double>();
		if(text != null && !text.isEmpty()) {
			char[] wordArray = text.toCharArray();
			if(wordArray != null && wordArray.length != 0) {
				for(char word : wordArray) {
					Double value = wordValueMap.get(word);
					if(value != null) {
						valueList.add(value);
					} else {
						System.out.println("-----word: " + word + " value is null. ");
					}
				}
			}
			if(!valueList.isEmpty()) {
				valueArray = new double[valueList.size()];
				for(int size = 0; size < valueList.size(); size++) {
					valueArray[size] = valueList.get(size);
				}
			}
		}
		return valueArray;
	}
	
	
	
	
	public static LabeledPoint labelText(int label, String text) {
		LabeledPoint labeledPoint = null;
		double[] textValueArray = textValueArray(text);
		if(textValueArray != null && textValueArray.length > 0) {
			labeledPoint = new LabeledPoint(Double.valueOf(label), Vectors.dense(textValueArray));
		}
		return labeledPoint;
	}
	
	public static List<LabeledPoint> labeledList(Map<String, Integer> textLabelMap) {
		List<LabeledPoint> labeledList = new ArrayList<LabeledPoint>();
		if(textLabelMap != null && !textLabelMap.isEmpty()) {
			for(String text : textLabelMap.keySet()) {
				Integer label = textLabelMap.get(text);
				LabeledPoint labeledPoint = labelText(label, text);
				if(labeledPoint != null) {
					labeledList.add(labeledPoint);
				}
			}
		}
		return labeledList;
	}
	
	

	public static void main(String[] args) {
		JavaSparkContext javaSparkContext = JavaRDDSparkContextMain.javaSparkContext("SparkMLTextAnalyze", "local[*]");
		
		Map<String, Integer> textLabelMap = new LinkedHashMap<String, Integer>();
		textLabelMap.put("出售发票", 1);
		textLabelMap.put("中奖通知", 1);
		textLabelMap.put("你好明天", 0);
		textLabelMap.put("今天任务", 0);
		textLabelMap.put("欢迎大家", 0);
		
		List<LabeledPoint> trainingLabelList = labeledList(textLabelMap);
		JavaRDD<LabeledPoint> trainingData = javaSparkContext.parallelize(trainingLabelList);
		// JavaRDD<LabeledPoint> trainingData = MLUtils.loadLibSVMFile(javaSparkContext.sc(), "data/mllib/sample_libsvm_data.txt").toJavaRDD();
		System.out.println("-----trainingData count: " + trainingData.count());
		System.out.println("-----trainingData take 10: " + trainingData.take(10));
		
		Map<String, Integer> verifyLabelMap = new LinkedHashMap<String, Integer>();
		verifyLabelMap.put("发票出售", 1);
		verifyLabelMap.put("欢迎今天", 0);
		verifyLabelMap.put("中奖通知", 1);
		verifyLabelMap.put("你家任务", 0);
		List<LabeledPoint> verifyLabelList = labeledList(verifyLabelMap);
		JavaRDD<LabeledPoint> verifyData = javaSparkContext.parallelize(verifyLabelList);
		System.out.println("----- verifyData count: " + verifyData.count());
		System.out.println("----- verifyData take 10: " + verifyData.take(10));
		
		final NaiveBayesModel naiveBayesModel = NaiveBayes.train(trainingData.rdd(), 1.0);
		System.out.println("-----naiveBayesModel: " + naiveBayesModel);
		
		JavaPairRDD<Double, Double> verifyPrediction = verifyData.mapToPair(labeledPoint -> {
			Vector features = labeledPoint.features();
			double prediction = naiveBayesModel.predict(features);
			System.out.println("-----features: " + Arrays.toString(features.toArray()) 
					+ "    predcition: " + prediction + "  label: " + labeledPoint.label());
			Tuple2<Double, Double> tuple2 = new Tuple2<Double, Double>(prediction, labeledPoint.label());
			return tuple2;
		});
		System.out.println("-----verifyPrediction count: " + verifyPrediction.count());
		 
		double accuracy = verifyPrediction.filter(tuple2 -> {
			boolean result = tuple2._1().equals(tuple2._2());
			return result;
		}).count();
		System.out.println("-----accuracy: " + accuracy);
		
		double[] pi = naiveBayesModel.pi();
		String modelType = naiveBayesModel.modelType();
		System.out.println("-----naiveBayesModel modelType: " + modelType + "  pi: " + Arrays.toString(pi));

	}

}
