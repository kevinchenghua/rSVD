package com.randomizedsvd

import java.io.{File, FileWriter, PrintWriter}
import org.apache.spark.mllib.linalg.distributed.{MatrixEntry, RowMatrix, IndexedRowMatrix, IndexedRow}

object OutputHelper {
  class ExecutionRecorder(file: String) {
    private val f:File = new File(file)
    private val writer = new FileWriter(f)
    private val recorder = new PrintWriter(writer)
    
    def finish(){
      recorder.close()
    }
    def writHeadline(hierarchy: Int = 0, subject: String){
      val headline: String = subject match {
        case "rsvd" => "Rsvd process:"
        case "combine" => "Combining process:"
        case _ => "Not found the subject"
      }
      recorder.write(" "*(hierarchy*2) + headline + "\n")
    }
    def startRsvd(hierarchy: Int = 1, rsvdNumber: Int) {
      recorder.write(" "*(hierarchy*2) + rsvdNumber.toString + " rsvd computing start.\n")
      ExecutionTimeTracker.startTracking()
    }
    def endRsvd(hierarchy: Int = 1, rsvdNumber: Int) {
      ExecutionTimeTracker.endTracking()
      val time = ExecutionTimeTracker.getExecutionTime()
      recorder.write(" "*(hierarchy*2) + rsvdNumber.toString + " rsvd finished with: " + time +" msec\n")
      println(rsvdNumber.toString + " rsvd finished with: " + time +" msec\n")
    }
    def startUpdateLoop(hierarchy: Int = 1, updateNumber: Int) {
      recorder.write(" "*(hierarchy*2) + updateNumber.toString + " update loop computing start.\n")
      ExecutionTimeTracker.startTracking()
    }
    def endUpdateLoop(hierarchy: Int = 1, updateNumber: Int) {
      ExecutionTimeTracker.endTracking()
      val time = ExecutionTimeTracker.getExecutionTime()
      recorder.write(" "*(hierarchy*2) + updateNumber.toString + " update loop finished with: " + time +" msec\n")
      println(updateNumber.toString + " update loop finished with: " + time +" msec\n")
    }
    def startStepLoop(hierarchy: Int = 2, updateNumber: Int) {
      recorder.write(" "*(hierarchy*2) + updateNumber.toString + " step update computing start.\n")
      ExecutionTimeTracker.startLoopTracking()
    }
    def endStepLoop(hierarchy: Int = 2, updateNumber: Int) {
      ExecutionTimeTracker.endLoopTracking()
      val time = ExecutionTimeTracker.getLoopExecutionTime()
      recorder.write(" "*(hierarchy*2) + updateNumber.toString + " step update finished with: " + time +" msec\n")
      println(updateNumber.toString + " step update finished with: " + time +" msec\n")
    }
    object ExecutionTimeTracker {
      private var startingTime = 0L
      private var endingTime = 0L
      private var flag = false
      
      private var loopStartingTime = 0L
      private var loopEndingTime = 0L
      private var loopFlag = false
      
      def startTracking() {
        if(flag == true) throw new Exception("The execution time tracker is already started!")
        flag = true
        startingTime = System.currentTimeMillis()
      }
      def endTracking() {
        if(flag == false) throw new Exception("The execution time tracker is already ended!")
        endingTime = System.currentTimeMillis()
        flag = false
      }
      def getExecutionTime() :Long = {
        if(flag == true) throw new Exception("The execution time tracker is runing!")
        endingTime - startingTime
      }
      def startLoopTracking() {
        if(loopFlag == true) throw new Exception("The execution time tracker is already started!")
        loopFlag = true
        loopStartingTime = System.currentTimeMillis()
      }
      def endLoopTracking() {
        if(loopFlag == false) throw new Exception("The execution time tracker is already ended!")
        loopEndingTime = System.currentTimeMillis()
        loopFlag = false
      }
      def getLoopExecutionTime() :Long = {
        if(loopFlag == true) throw new Exception("The execution time tracker is runing!")
        loopEndingTime - loopStartingTime
      }
    }
  }
  class MatrixDocumenter(file: String) {
    private val f:File = new File(file)
    private val writer = new FileWriter(f)
    private val documenter = new PrintWriter(writer)
    def writeMatrix(mat: IndexedRowMatrix) {
      val rows = mat.rows.sortBy(row => row.index, ascending = true).collect()
      rows.map(row => row.vector.toArray.mkString("\t")).foreach(row => documenter.write(row+"\n"))
      documenter.close()
    }
  }
}