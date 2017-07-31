package com.highill.practice.spark.mllib.rdd.dimensionality.reduction;

import java.util.LinkedList;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.linalg.distributed.RowMatrix;

import com.highill.practice.spark.JavaRDDSparkContextMain;

/**
 * 
 * http://spark.apache.org/docs/2.1.0/mllib-dimensionality-reduction.html#principal-component-analysis-pca
 *
 */
public class SparkMLDimensionalityReductionPrincipalComponentAnalysis {

	public static void main(String[] args) {
		JavaSparkContext javaSparkContext = JavaRDDSparkContextMain.javaSparkContext("SparkMLDimensionalityReductionPrincipalComponentAnalysis", "local[*]");
		double[][] valueArray = new double[][]{
				{1.12, 2.05, 3.12}, {5.56, 6.28, 8.94}, {10.2, 8.0, 20.5}	
		};
		
		LinkedList<Vector> vectorList = new LinkedList<Vector>();
		for(int size = 0; size < valueArray.length; size++) {
			Vector currentRow = Vectors.dense(valueArray[size]);
			vectorList.add(currentRow);
		}
		
		JavaRDD<Vector> vectorData = javaSparkContext.parallelize(vectorList);
		System.out.println("-----vectorData count: " + vectorData.count());
		
		RowMatrix rowMatrix = new RowMatrix(vectorData.rdd());
		System.out.println("-----rowMatrix: " + rowMatrix);
		System.out.println("-----rowMatrix numCols: " + rowMatrix.numCols() + ",    numRows: " + rowMatrix.numRows());
		
		int k = 3;
		Matrix principalComponent = rowMatrix.computePrincipalComponents(k);
		System.out.println("-----principalComponent: \n" + principalComponent);
		System.out.println("-----principalComponent numCols: " + principalComponent.numCols() 
				+ ",    numRows: " + principalComponent.numRows());
		
		
		RowMatrix project = rowMatrix.multiply(principalComponent);
		System.out.println("-----project: " + project);
		System.out.println("-----project numCols: " + project.numCols() + ",    numRows: " + project.numRows());

	}

}
