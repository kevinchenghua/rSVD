import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.rdd.RDD

object RunTest {
  def main(args: Array[String]): Unit = {
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
    println("test!!!!!!!!!!!")
  }
}