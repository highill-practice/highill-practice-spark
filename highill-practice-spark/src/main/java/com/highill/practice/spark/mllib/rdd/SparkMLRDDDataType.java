package com.highill.practice.spark.mllib.rdd;


import java.util.Arrays;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.Matrices;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.mllib.linalg.QRDecomposition;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.linalg.distributed.RowMatrix;
import org.apache.spark.mllib.regression.LabeledPoint;

import com.highill.practice.spark.JavaRDDSparkContextMain;
/**
 * http://spark.apache.org/docs/2.1.0/mllib-data-types.html
 * 
 *
 */
public class SparkMLRDDDataType {

	public static void main(String[] args) {
		
		// Local vector
		Vector vector = Vectors.dense(1.0, 0.0, 3.0, 0.0);
		System.out.println("-----vector: " + vector);
		
		// requirement failed: 
		// Sparse vectors require that the dimension of the indices match the dimension of the values. You provided 2 indices and  5 values.
		Vector vectorSparse = Vectors.sparse(10, new int[]{1, 3, 4, 6, 8, 9}, new double[]{1.0, 3.0, 0.0, 0.1, 2.0, 2.2});
		System.out.println("-----vectorSparse: " + vectorSparse);
		
		
		// Labeled point
		LabeledPoint labeledPoint = new LabeledPoint(1.0, Vectors.dense(1.0, 0.0, 3.0));
		System.out.println("-----labeledPoint: " + labeledPoint);
		LabeledPoint labeledPointSparse = new LabeledPoint(1.1, Vectors.sparse(6, new int[]{1, 2, 3}, new double[]{1.0, 2.0, 2.1}));
		System.out.println("-----labeledPointSparse: " + labeledPointSparse);
		
		// Matrix  
		Matrix matrix = Matrices.dense(3,  2, new double[]{1.0, 2.0, 3.0, 0.5, 0.8, 1.1});
		System.out.println("-----matrix: \n" + matrix);
		Matrix matrixSparse = Matrices.sparse(3, 2, new int[]{1, 2, 3},  new int[]{3, 6, 9}, new double[]{2, 4, 6});
		System.out.println("-----matrixSparse: \n" + matrixSparse);
		
		JavaSparkContext sparkContext = JavaRDDSparkContextMain.javaSparkContext("Spark ML RDD DataType", "local[*]");
		// RowMatrix  
		JavaRDD<Vector> javaRDDVector = sparkContext.parallelize(Arrays.asList(
				Vectors.dense(1.0, 3.0, 5.5, 11.1), Vectors.dense(0.1, 0.5, 1.6, 3.3)));
		System.out.println("-----javaRDDVector: " + javaRDDVector.collect());
		RowMatrix rowMatrix = new RowMatrix(javaRDDVector.rdd());
		System.out.println("-----rowMatrix: " + rowMatrix);
		long rowMatrixRowNum = rowMatrix.numRows();
		long colMatrixColNum = rowMatrix.numCols();
		System.out.println("-----rowMatrixRowNum: " + rowMatrixRowNum + ",  colMatrixColNum: " + colMatrixColNum);
		
		// requirement failed: Dimension mismatch: 4 vs 2
		QRDecomposition<RowMatrix, Matrix> decomposition = rowMatrix.tallSkinnyQR(false);
		System.out.println("-----decomposition: " + decomposition);

		// IndexedRowMatrix
		
		
		// CoordinateMatrix  
		
		
		// BlockMatrix  
		
	}

}
