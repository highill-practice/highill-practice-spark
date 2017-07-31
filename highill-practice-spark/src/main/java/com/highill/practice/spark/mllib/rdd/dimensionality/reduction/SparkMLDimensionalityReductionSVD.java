package com.highill.practice.spark.mllib.rdd.dimensionality.reduction;

import java.util.LinkedList;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.mllib.linalg.SingularValueDecomposition;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.linalg.distributed.RowMatrix;

import com.highill.practice.spark.JavaRDDSparkContextMain;

/**
 * 
 * http://spark.apache.org/docs/2.1.0/mllib-dimensionality-reduction.html#svd-example
 *
 */
public class SparkMLDimensionalityReductionSVD {

	public static void main(String[] args) {
		JavaSparkContext javaSparkContext = JavaRDDSparkContextMain.javaSparkContext("SparkMLDimensionalityReductionSVD", "local[*]");
		
		double[][] valueArray = new double[][]{
				{1.12, 2.05, 3.12}, {5.56, 6.28, 8.94}, {10.2, 8.0, 20.5}	
		};
		LinkedList<Vector> rowsList = new LinkedList<Vector>();
		for(int size = 0; size < valueArray.length; size++) {
			Vector currentRow = Vectors.dense(valueArray[size]);
			rowsList.add(currentRow);
		}
		
		JavaRDD<Vector> vectorData = javaSparkContext.parallelize(rowsList);
		System.out.println("-----vectorData count: " + vectorData.count());
		
		RowMatrix rowMatrix = new RowMatrix(vectorData.rdd());
		System.out.println("-----rowMatrix numClos: " + rowMatrix.numCols() + ",  numRows: " + rowMatrix.numRows());
		
		SingularValueDecomposition<RowMatrix, Matrix> singularValueDecomposition = 
				rowMatrix.computeSVD(3, true, 1.0E-9d);
		System.out.println("-----singularValueDecomposition: " + singularValueDecomposition);
		RowMatrix u = singularValueDecomposition.U();
		Vector s = singularValueDecomposition.s();
		Matrix v = singularValueDecomposition.V();
		System.out.println("-----u numCols: " + u.numCols() + ",  numRows: " + u.numRows() 
				+ ",  s size: " + s.size() 
				+ ",  v numCols: " + v.numCols() + ",   numRows: " + v.numRows());
		System.out.println("----- u: \n" + u);
		System.out.println("----- s: " + s);
		System.out.println("----- v: \n" + v);
		

	}

}
