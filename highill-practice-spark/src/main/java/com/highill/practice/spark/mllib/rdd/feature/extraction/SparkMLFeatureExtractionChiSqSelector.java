package com.highill.practice.spark.mllib.rdd.feature.extraction;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.feature.ChiSqSelector;
import org.apache.spark.mllib.feature.ChiSqSelectorModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.util.MLUtils;






import com.highill.practice.spark.JavaRDDSparkContextMain;

public class SparkMLFeatureExtractionChiSqSelector {

	public static void main(String[] args) {
		JavaSparkContext javaSparkContext = JavaRDDSparkContextMain.javaSparkContext("SparkMLFeatureExtractionChiSqSelector", "local[*]");
		SparkContext sparkContext = javaSparkContext.sc();
		
		String dataPath = "data/mllib/sample_libsvm_data.txt";
		JavaRDD<LabeledPoint> labeledData = MLUtils.loadLibSVMFile(sparkContext, dataPath).toJavaRDD().cache();
		System.out.println("-----labeledData count: " + labeledData.count());
		
		// Discretize data in 16 equals bins since ChiSqSelector requires categorical features  
		// Althought features are doubles, the ChiSqSelector treats each unique value as category
		JavaRDD<LabeledPoint> discretizedData = labeledData.map(labeledPoint -> {
			final double[] discretizedFeatures = new double[labeledPoint.features().size()];
			for(int size = 0; size < labeledPoint.features().size(); size++) {
				discretizedFeatures[size] = Math.floor(labeledPoint.features().apply(size) / 16);
			}
			LabeledPoint labeledValue = new LabeledPoint(labeledPoint.label(), Vectors.dense(discretizedFeatures));
			return labeledValue;
		});
		System.out.println("-----discretizedData count: " + discretizedData.count());
		
		ChiSqSelector chiSqSelector = new ChiSqSelector(50);
		final ChiSqSelectorModel chiSqSelectorModel = chiSqSelector.fit(discretizedData.rdd());
		System.out.println("-----chiSqSelectorModel: " + chiSqSelector);
		
		JavaRDD<LabeledPoint> filteredData = discretizedData.map(labeledPoint -> {
			Vector vector = chiSqSelectorModel.transform(labeledPoint.features());
			LabeledPoint labeledFilter = new LabeledPoint(labeledPoint.label(), vector);
			return labeledFilter;
		});
		System.out.println("-----filteredData count: " + filteredData.count());

	}

}
