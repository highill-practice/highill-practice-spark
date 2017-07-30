package com.highill.practice.spark.mllib.rdd.collaborative.filtering;

import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.recommendation.ALS;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.mllib.recommendation.Rating;

import scala.Tuple2;

import com.highill.practice.spark.JavaRDDSparkContextMain;

/**
 * 
 * https://github.com/apache/spark/blob/master/examples/src/main/java/org/apache/spark/examples/mllib/JavaRecommendationExample.java  
 * 
 * http://spark.apache.org/docs/2.1.0/mllib-collaborative-filtering.html
 *
 */
public class SparkMLCollaborativeFiltering {

	public static void main(String[] args) {
		JavaSparkContext javaSparkContext = JavaRDDSparkContextMain.javaSparkContext("SparkMLCollaborativeFiltering", "local[*]");
		// SparkContext sparkContext = javaSparkContext.sc();
		
		String dataPath = "data/mllib/als/test.txt";
		JavaRDD<String> fileData = javaSparkContext.textFile(dataPath);
		JavaRDD<Rating> ratingData = fileData.map(line -> {
			String[] stringArray = line.split(",");
			Rating rating = new Rating(Integer.parseInt(stringArray[0]), Integer.parseInt(stringArray[1]), Double.parseDouble(stringArray[2]));
			return rating;
		});
		System.out.println("-----ratingDataCount: " + ratingData.count());
		
		int rank = 10;
		int numIterations = 10;
		MatrixFactorizationModel matrixFactorizationModel = ALS.train(JavaRDD.toRDD(ratingData), rank, numIterations, 0.01);
		System.out.println("-----matrixFactorizationModel: " + matrixFactorizationModel);
		
		JavaRDD<Tuple2<Object, Object>> userProducts = ratingData.map(rating -> {
			int user = rating.user();
			int product = rating.product();
			Tuple2<Object, Object> tuple2 = new Tuple2<Object, Object>(user, product);
			return tuple2;
		});
		System.out.println("-----userProducts count: " + userProducts.count());
		
		JavaPairRDD<Tuple2<Integer, Integer>, Double> predictions = JavaPairRDD.fromJavaRDD(
				matrixFactorizationModel.predict(JavaRDD.toRDD(userProducts)).toJavaRDD().map(rating -> {
					int user = rating.user();
					int product = rating.product();
					double rateValue = rating.rating();
					Tuple2<Tuple2<Integer, Integer>, Double> tuple2 = new Tuple2<Tuple2<Integer, Integer>, Double>(new Tuple2<Integer, Integer>(user, product), rateValue);
					return tuple2;
				})
				);
		System.out.println("-----predictions count: " + predictions.count());
		
		JavaRDD<Tuple2<Double, Double>> ratesAndPredictions = JavaPairRDD.fromJavaRDD(
				ratingData.map(rating -> {
					int user = rating.user();
					int product = rating.product();
					double rateValue = rating.rating();
					Tuple2<Tuple2<Integer, Integer>, Double> tuple2 = new Tuple2<Tuple2<Integer, Integer>, Double>(new Tuple2<Integer, Integer>(user, product), rateValue);
					return tuple2;
				})
				).join(predictions).values();
		System.out.println("-----ratesAndPredictions count: " + ratesAndPredictions.count());
		
		Double meanSquaredError = JavaDoubleRDD.fromRDD( ratesAndPredictions.map(tuple2 -> {
			Double error = tuple2._1() - tuple2._2();
			return (Object)(error * error);
		}).rdd() 
		).mean();
		System.out.println("-----meanSquaredError: " + meanSquaredError);
		
		

	}

}
