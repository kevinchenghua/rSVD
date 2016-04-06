package com.randomizedsvd

import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.rdd.RDD
import _root_.com.jmatio.io.MatFileReader
import _root_.com.jmatio.types.MLDouble
import _root_.com.jmatio.types.MLArray
import org.apache.spark.mllib.linalg.{ Vectors, Vector, Matrix, SingularValueDecomposition}
import org.apache.spark.mllib.linalg.distributed.{MatrixEntry, RowMatrix, IndexedRowMatrix, CoordinateMatrix, BlockMatrix, IndexedRow}
import org.apache.spark.mllib.random.RandomRDDs



object RunTest {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    //// Set Spark
    val conf = new SparkConf()
             .setMaster("spark://master:7077")
             .setAppName("RSVD_TEST_APP")
             .set("spark.executor.memory", "1g")
             .set("spark.eventLog.enabled", "false")
    val sc = new SparkContext(conf)
    
    //// Set parameters
    // # of images
    val m = 400
    // # of rows/cols of each images
    val p = 64 
    val q = 64 
    // # of singular values to pick
    val r = 50
    // # of rows/cols to pick in folding version
    val a = 10
    val b = 10
    // # of random times in random version
    val n_rand = 100  
    //Set Image (all algorithm will focus on right singular vector)
    val file = new MatFileReader("matrices/image.mat")
    val content = file.getContent
    val image = content.get("Image").asInstanceOf[MLDouble].getArray
    // Transfer matrix to distributed style
    val rows = sc.parallelize(image.indices.zip(image.map(Vectors.dense(_))).
        map(row => IndexedRow(row._1, row._2)))
    val mat: IndexedRowMatrix = new IndexedRowMatrix(rows)
    //Compute randomized SVD
    val recorder = new OutputHelper.ExecutionRecorder("./output/random.txt")
    val UU:(IndexedRowMatrix,BlockMatrix) = RandomizedSingularValueDecomposition.rsvdSeqUU(sc, recorder, mat, r = r, n = 1)
    val test = RandomizedSingularValueDecomposition.combineOrth(sc, recorder, UU._1, UU._2, 1)
    recorder.finish()
    //Output the combined matrix
    val matrixDocumenter = new OutputHelper.MatrixDocumenter("./output/rsvd.txt")
    matrixDocumenter.writeMatrix(test.toIndexedRowMatrix())
    //Output the full svd matrix
    //val fullSVDDocumenter = new OutputHelper.MatrixDocumenter("./output/fullsvd.txt")
    //fullSVDDocumenter.writeMatrix(mat.computeSVD(r, computeU = true).U)
  }
}