package com.highill.practice.spark;

import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.BlockJUnit4ClassRunner;


@RunWith(BlockJUnit4ClassRunner.class)
public class SparkContextVerify {
	
	
	@Test
	public void sparkContext() {
		String appName = "highill-practice-sprak-junit";
		String master = "local[*]";
		JavaSparkContext sparkContext = JavaRDDSparkContextMain.javaSparkContext(appName, master);
		
		System.out.println("-----sparkContext: " + sparkContext);
		System.out.println("-----sparkHome: " + sparkContext.getSparkHome());
		
	}

}
