package com.highill.practice.spark.mllib.rdd.clustering;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.clustering.LDA;
import org.apache.spark.mllib.clustering.LDAModel;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;

import scala.Tuple2;

import com.highill.practice.spark.JavaRDDSparkContextMain;

public class SparkMLClusteringLatentDirichletAllocation {

	public static void main(String[] args) {
		JavaSparkContext javaSparkContext = JavaRDDSparkContextMain.javaSparkContext("SparkMLClusteringLatentDirichletAllocation", "local[*]");
		String dataPath = "data/mllib/sample_lda_data.txt";
		JavaRDD<String> fileData = javaSparkContext.textFile(dataPath);
		JavaRDD<Vector> parsedData = fileData.map(line -> {
			String[] stringArray = line.split(" ");
			double[] valueArray = new double[stringArray.length];
			for(int size = 0; size < stringArray.length; size++) {
				valueArray[size] = Double.valueOf(stringArray[size]);
			}
			Vector vector = Vectors.dense(valueArray);
			return vector;
		});
		System.out.println("-----fileData count: " + fileData.count() + ",    parsedData count: " + parsedData.count());
		
		JavaPairRDD<Long, Vector> corpus = JavaPairRDD.fromJavaRDD(
				parsedData.zipWithIndex().map(tuple -> {
			Tuple2<Long, Vector> tuple2 = tuple.swap();
			return tuple2;
		})
		);
		corpus.cache();
		System.out.println("-----corpus: " + corpus.count());
		
		int k = 3;
		LDAModel ldaModel = new LDA().setK(k).run(corpus);
		System.out.println("-----ldaModel: " + ldaModel);
		
		System.out.println("-----Learned topics (as distibutions over vocab of " + ldaModel.vocabSize() + " words) : ");
		Matrix topics = ldaModel.topicsMatrix();
		for(int topic = 0; topic < ldaModel.k(); topic++) {
			System.out.println("-----Topic " + topic + ": ");
			for(int word = 0; word <ldaModel.vocabSize(); word++) {
				System.out.print(" " + topics.apply(word, topic));
			}
			System.out.println("\n");
		}

	}

}
