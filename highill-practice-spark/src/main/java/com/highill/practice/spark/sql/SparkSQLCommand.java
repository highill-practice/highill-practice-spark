package com.highill.practice.spark.sql;

import java.util.Properties;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.highill.practice.spark.JavaRDDSparkContextMain;

public class SparkSQLCommand {

	public static void main(String[] args) {
		
		JavaSparkContext sparkContext = JavaRDDSparkContextMain.javaSparkContext("Spark_SQL_Test", "local[*]"); 
		SparkSession sparkSession = SparkSession.builder().appName("Spark_SQL_Test").config(sparkContext.getConf()).getOrCreate();
		System.out.println("-----sparkSession: " + sparkSession);
		
		
		// jdbc:mysql://192.168.1.104:3306/demo_database?useUnicode=true&characterEncoding=utf8&autoReconnect=true
		String jdbcUrl = "jdbc:mysql://192.168.1.104:3306/demo_database";
		String jdbcTable = "demo_person";
		String jdbcUser = "github_demo";
		String jdbcPassword = "GitHubDemo2017!";
		
		Dataset<Row> sparkLoadData = sparkSession.read().format("jdbc")
				.option("url", jdbcUrl)
				.option("dbtable", jdbcTable)
				.option("user", jdbcUser)
				.option("password", jdbcPassword)
				.load();
		System.out.println("-----sparkLoadData: " + sparkLoadData.javaRDD().collect());
		
		
		Properties jdbcProperties = new Properties();
		jdbcProperties.put("user", "github_demo");
		jdbcProperties.put("password", "GitHubDemo2017!");
		jdbcProperties.put("useUnicode", "true");
		jdbcProperties.put("characterEncoding", "utf8");
		jdbcProperties.put("Reconnect", "true");
		Dataset<Row> sparkJdbcData = sparkSession.read().jdbc(jdbcUrl, jdbcTable, jdbcProperties);
		System.out.println("-----sparkJdbcData: " + sparkJdbcData.javaRDD().collect());
		
		
		

	}

}
