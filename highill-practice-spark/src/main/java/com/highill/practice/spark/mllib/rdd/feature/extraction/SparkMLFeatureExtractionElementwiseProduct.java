package com.highill.practice.spark.mllib.rdd.feature.extraction;

import java.util.Arrays;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.feature.ElementwiseProduct;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;

import com.highill.practice.spark.JavaRDDSparkContextMain;

/**
 * 
 * http://spark.apache.org/docs/2.1.0/mllib-feature-extraction.html#elementwiseproduct
 *
 */
public class SparkMLFeatureExtractionElementwiseProduct {

	public static void main(String[] args) {
		JavaSparkContext javaSparkContext = JavaRDDSparkContextMain.javaSparkContext("SparkMLFeatureExtractionElementwiseProduct", "local[*]");
		JavaRDD<Vector> vectorData = javaSparkContext.parallelize(Arrays.asList(
				Vectors.dense(1.0, 2.0, 3.0), Vectors.dense(4.0, 5.0, 6.0), Vectors.dense(7.0, 11.0, 20.0)
				));
		System.out.println("-----vectorData count: " + vectorData.count());
		
		Vector transformingVector = Vectors.dense(1.0, 2.0, 3.0);
		
		final ElementwiseProduct elementwiseProduct = new ElementwiseProduct(transformingVector);
		System.out.println("-----elementwiseProduct: " + elementwiseProduct);
		
		JavaRDD<Vector> transformedData = elementwiseProduct.transform(vectorData);
		JavaRDD<Vector> transformedMapData = vectorData.map(vector -> {
			Vector transformed = elementwiseProduct.transform(vector);
			return transformed;
		});
		System.out.println("-----transformedData count: " + transformedData.count());
		System.out.println("-----transformedData: " + transformedData.collect());
		System.out.println("-----transformedMapData count: " + transformedMapData.count());
		System.out.println("-----transformedMapData: " + transformedMapData.collect());

	}

}
