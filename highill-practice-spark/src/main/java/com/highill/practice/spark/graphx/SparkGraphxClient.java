package com.highill.practice.spark.graphx;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.graphx.EdgeRDD;
import org.apache.spark.graphx.Graph;
import org.apache.spark.graphx.GraphLoader;
import org.apache.spark.graphx.VertexRDD;
import org.apache.spark.rdd.RDD;
import org.apache.spark.storage.StorageLevel;

import scala.Tuple2;

import com.highill.practice.spark.JavaRDDSparkContextMain;

/**
 *  http://spark.apache.org/docs/2.1.0/graphx-programming-guide.html#pagerank
 *  
 * 
 *
 */
public class SparkGraphxClient {

	public static void main(String[] args) {
		
		// https://github.com/apache/spark/blob/master/data/graphx/followers.txt
		// https://github.com/apache/spark/blob/master/data/graphx/users.txt
		JavaSparkContext sparkContext = JavaRDDSparkContextMain.javaSparkContext("Spark_Graphx_Client", "local[*]");
		Graph<Object, Object> graph = GraphLoader.edgeListFile(sparkContext.sc(), "data/graphx/followers.txt", false, -1, StorageLevel.MEMORY_ONLY(), StorageLevel.MEMORY_ONLY());
		System.out.println("-----graph: " + graph);
		// System.out.println("-----graph edges(): " + graph.edges());
		
		// graph.aggregateMessages(EdgeContext., mergeMsg, TripletFields.All, evidence$11)
		EdgeRDD<Object> edgeRDD = graph.edges();
		System.out.println("-----graph edgeRDD: " + edgeRDD);
		System.out.println("-----graph edgeRDD count: " + edgeRDD.count());
		
		VertexRDD<Object> vertexRDD = graph.vertices();
		System.out.println("-----graph vertexRDD: " + vertexRDD);
		System.out.println("-----graph vertexRDD count: " + vertexRDD.count());
		System.out.println("-----graph vertexRDD collect: " + vertexRDD.collect());
		System.out.println("-----graph vertexRDD collectPartitions: " + vertexRDD.collectPartitions());
		
		
		JavaRDD<String> usersFileRDD = sparkContext.textFile("data/graphx/users.txt");
		System.out.println("-----usersFileRDD: " + usersFileRDD);
		System.out.println("-----usersFileRDD collect: " + usersFileRDD.collect());
		
		
		JavaRDD<Object> userRDD = usersFileRDD.map(line -> {
			String[] valueArray = line.split(",");
			Long userId = Long.valueOf(valueArray[0]);
			String username = valueArray[1];
			
			User user = new User();
			user.setUserId(userId);
			user.setUsername(username);
			return user;
		});
		System.out.println("-----userRDD: " + userRDD);
		System.out.println("-----userRDD collect: " + userRDD.collect());
		
		
		// TODO, Java API not the same as Scale. 
		RDD<Object>unionRDD = userRDD.rdd().union(vertexRDD.glom());
		System.out.println("-----unionRDD:" + unionRDD);
		System.out.println("-----unionRDD count: " + unionRDD.count());
		System.out.println("-----unionRDD collect: " + unionRDD.collect());
		System.out.println("-----unionRDD take all: " +  unionRDD.take(Long.valueOf(unionRDD.count()).intValue()));
		JavaRDD<Object> unionJavaRDD = unionRDD.toJavaRDD();
		System.out.println("-----unionJavaRDD: " + unionJavaRDD);
		System.out.println("-----unionJavaRDD count: " + unionJavaRDD.count());
		System.out.println("-----unionJavaRDD collect: " + unionJavaRDD.collect());
		JavaRDD<User> rankUserRDD = unionJavaRDD.map(o -> {
			if (o instanceof User) {
				User user = (User) o;
				return user;

			} else {
				System.out.println("-----o: " + o + ", o class is ");
			}
			return null;
		});
		System.out.println("-----rankUserRDD: " + rankUserRDD);
		System.out.println("-----rankUserRDD collect: " + rankUserRDD.collect());
		
		

	}
	


}
