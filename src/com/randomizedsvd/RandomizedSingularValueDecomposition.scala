package com.randomizedsvd

import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.rdd.{ RDD, PairRDDFunctions }
import org.apache.spark.mllib.linalg.{ Vectors, Vector, Matrix, DenseMatrix, DenseVector, SingularValueDecomposition}
import org.apache.spark.mllib.linalg.distributed.{MatrixEntry, RowMatrix, IndexedRowMatrix, IndexedRow, BlockMatrix, CoordinateMatrix}
import org.apache.spark.mllib.random.RandomRDDs

object RandomizedSingularValueDecomposition {
  def randomIndexedRowMatrix(sc: SparkContext, m:Long, n:Int): IndexedRowMatrix = { 
    def addRowIndex(r: RDD[Vector]): RDD[IndexedRow] = {
      val sc = r.sparkContext
      val partitionSizes = r.mapPartitionsWithIndex((index, rows) => Iterator((index, rows.size))).collect()
      val partitionGlobalStartIndex = partitionSizes.sortBy(_._1).map(_._2).scanLeft(0)(_+_)
      
      val startIndexes = sc.broadcast(partitionGlobalStartIndex)
      r.mapPartitionsWithIndex((partitionIndex, rows) => {
        val partitionStartIndex = startIndexes.value(partitionIndex)
        rows.zipWithIndex map {
          case (row, rowIndex) => IndexedRow(partitionStartIndex + rowIndex, row)
        }
      })
    }
    
    val normalizedFactor = scala.math.sqrt(m)
    new IndexedRowMatrix(addRowIndex(RandomRDDs.normalVectorRDD(sc, m, n).map(x => Vectors.dense(x.toArray.map(y => y/normalizedFactor)))))  
  }
  
  def rsvd(sc: SparkContext, A:IndexedRowMatrix, r: Int, t: Int = 0, 
      Omega: Option[IndexedRowMatrix] = None): SingularValueDecomposition[IndexedRowMatrix, Matrix]={
    /*-----------------------------------------------
 		Randomized truncated SVD
		Input:
			A: m*n matrix to decompose
			r: the number of singular values to keep (rank of S)
			t (optional): the number used in power (default = 0)
		  Omega (optional): the projection matrix to use in sampling
	                      (program will compute A * Omega for direction = 0
	                                            Omega * A for direction = 1)
 
	  Output: classical output as the builtin svd matlab function
    -----------------------------------------------*/
    
    // Set necessary variables
    val m: Long = A.numRows()
    val n: Long = A.numCols()
    val omega = Omega match {
      case Some(mat) => mat
      case None => randomIndexedRowMatrix(sc, n, r)
    }
    
    // Convert IndexedRowMatrix to BlockMatrix
    val A_block: BlockMatrix = A.toBlockMatrix()
    val omega_block: BlockMatrix = omega.toBlockMatrix()
     
    //Compute SVD
    val Y: BlockMatrix = A_block multiply omega_block
    val Q: BlockMatrix = Y.toIndexedRowMatrix().computeSVD(r, computeU = true).U.toBlockMatrix()
    val B: IndexedRowMatrix = Q.transpose.multiply(A_block).toIndexedRowMatrix()
    val svd_temp: SingularValueDecomposition[IndexedRowMatrix, Matrix] = B.computeSVD(r, computeU = true)
    val U_temp = new IndexedRowMatrix(svd_temp.U.rows)
    val U: IndexedRowMatrix = Q.multiply(U_temp.toBlockMatrix()).toIndexedRowMatrix()
    val s: Vector = svd_temp.s
    val V: Matrix = svd_temp.V
    
    SingularValueDecomposition(U, s, V)
  }
  def rsvdSeqUU(sc: SparkContext, recorder: OutputHelper.ExecutionRecorder, A:IndexedRowMatrix, r: Int, t: Int = 0, n: Int): (IndexedRowMatrix,BlockMatrix) ={
    def computeUU(U: IndexedRowMatrix): BlockMatrix = {
      U.toBlockMatrix() multiply U.toBlockMatrix().transpose
    }
    recorder.writHeadline(subject = "rsvd")
    recorder.startRsvd(rsvdNumber = 1)
    val W0 = rsvd(sc,A,r,t).U
    var UU = computeUU(W0)
    recorder.endRsvd(rsvdNumber = 1)
    for(i <- 1 to n-1){
      println("It's computing the "+(i+1)+"th rsvd")
      recorder.startRsvd(rsvdNumber = i+1)
      UU = UU add computeUU(rsvd(sc,A,r,t).U)
      recorder.endRsvd(rsvdNumber = i+1)
    }
    UU.cache()
    (W0,UU)
  }
  def combineOrth(sc: SparkContext, recorder: OutputHelper.ExecutionRecorder, W0: IndexedRowMatrix, UU: BlockMatrix, n: Int, toleranceT: Double = 1e-2, toleranceF: Double = 1e-4, iterationMax: Int = 100): BlockMatrix = {
    def eye(k: Int, entry: Double = 1.0): BlockMatrix = {
      val entries: RDD[MatrixEntry] = sc.makeRDD((0 until k) map { i => MatrixEntry(i,i,entry)})
      val eyeCoordinateMatrix: CoordinateMatrix = new CoordinateMatrix(entries)
      val eyeBlockMatrix: BlockMatrix = eyeCoordinateMatrix.toBlockMatrix()
      eyeBlockMatrix
    }
    def computeGF(W: BlockMatrix): BlockMatrix = {
      UU multiply W multiply eye(W.numCols().toInt, 1.0/n)
    }
    def computeTrace(mat: IndexedRowMatrix): Double = {
      mat.rows.map(row => row.vector(row.index.toInt)).reduce(_+_)
    }
    def computeInverse(mat: IndexedRowMatrix): IndexedRowMatrix = {
      val n = mat.numCols().toInt
      val svd = mat.computeSVD(k = n, computeU = true)
      val U = svd.U
      val transposeU = U.toBlockMatrix().transpose.toLocalMatrix()
      val inverseS = DenseMatrix.diag(new DenseVector(svd.s.toArray.map(x => math.pow(x,-1))))
      val V = svd.V
      //println("fffffffffffffffffff")
      val inverseMat = eye(n).toIndexedRowMatrix().multiply(V).multiply(inverseS).multiply(transposeU)
      inverseMat
    }
    def matrixJoin(mat1: BlockMatrix, mat2: BlockMatrix): BlockMatrix = {
      val mat1ColumnNumber = mat1.numCols()
      val mat1Transpose = mat1.transpose.toIndexedRowMatrix()
      val mat2Transpose = mat2.transpose.toIndexedRowMatrix()
      val joinedRows = mat1Transpose.rows ++ mat2Transpose.rows.map(row => IndexedRow(row.index + mat1ColumnNumber, row.vector))
      val joinedMatrixTranspose = new IndexedRowMatrix(joinedRows)
      joinedMatrixTranspose.toBlockMatrix().transpose
    }
    def updateLoop(W: BlockMatrix, r: Int, m: Int, oldF: Double, iterationNumber: Int):BlockMatrix = {      
      def updateHelper(U: BlockMatrix, V: BlockMatrix, GF: BlockMatrix, r: Int, m:Int , t: Double, oldF: Double, tracker: Int): (Double ,BlockMatrix) = {
        if(t < toleranceT){
          println("The step size is too small.")
          return (oldF,W)
        }
        println("It's the "+tracker.toString+"th update of the newW.")
        println("And the t now is: "+t.toString)
        println("The oldF is: "+oldF.toString)
        recorder.startStepLoop(updateNumber = tracker)
        val Y = (eye(m,1.0).multiply(W)).add( eye(m,-t/2).multiply((W.multiply((W.transpose).multiply(GF))).add(GF.multiply(eye(r,-1.0)))))
        val X = computeInverse((eye(2*r,-1.0).multiply(eye(2*r,1.0)).add(eye(2*r,t/2).multiply(V.transpose).multiply(U))).toIndexedRowMatrix()).toBlockMatrix() multiply V.transpose multiply Y
        val newW = Y.multiply(eye(r,1.0)).add(U multiply X multiply eye(r,-t/2))
        val newF = computeTrace((newW.transpose multiply UU multiply newW).toIndexedRowMatrix())
        if(newF > oldF) {
          recorder.endStepLoop(updateNumber = tracker)
          (newF, newW)
        }else {
          recorder.endStepLoop(updateNumber = tracker)
          updateHelper(U, V, GF, r, m, t/2, oldF, tracker+1)
        }
      }
      println("It's the "+(iterationNumber+1).toString+" update loop")
      recorder.startUpdateLoop(updateNumber = iterationNumber+1)
      val GF = computeGF(W)
      val U = matrixJoin(GF, W)
      val V = matrixJoin(W, GF multiply eye(GF.numCols().toInt,-1.0))
      val tupleFW = updateHelper(U, V, GF, r, m, 1.0, oldF,1)
      val newF = tupleFW._1
      val newW = tupleFW._2
      println("The update of the newW succeed")
      println("The newF is: "+newF.toString)
      if(iterationNumber >= iterationMax) {
        recorder.endUpdateLoop(updateNumber = iterationNumber+1)
        newW
      }else if(((newF-oldF)/oldF) < toleranceF) {
        recorder.endUpdateLoop(updateNumber = iterationNumber+1)
        W
      }else {
        recorder.endUpdateLoop(updateNumber = iterationNumber+1)
        updateLoop(newW,r,m,newF,iterationNumber+1)
      }
    }
    val r =W0.numCols().toInt
    val m = W0.numRows().toInt
    val W = W0.toBlockMatrix
    recorder.writHeadline(subject = "combine")
    updateLoop(W,r,m,computeTrace((computeGF(W).transpose.multiply(W)).toIndexedRowMatrix()),0)
  }
}