import org.apache.spark.ml.feature._
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}

import org.apache.spark.ml.{Pipeline}
import org.apache.spark.ml.classification._
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.sql.{Row, SparkSession}

object TimeSeries {

  /** Makes sure only ERROR messages get logged to avoid log spam. */
  def setupLogging() = {
    import org.apache.log4j.{Level, Logger}
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)
  }


  def main(args: Array[String]): Unit = {
    //val spark = new SparkConf().setAppName("AirlineTweets").setMaster("local[*]")

    val spark = SparkSession.builder
      .master("local")
      .appName("TimeSeries Web Traffic Forecasting")
      .getOrCreate

    import spark.implicits._

    //if (args.length < 2) {
    //  System.err.println("Correct usage: Program_Name inputPath outputPath")
    //  System.exit(1)
    //}

    // Get rid of log spam (should be called after the context is set up)
    setupLogging()

    val inputPath = "data/train_1.csv" // args(0)
    println("inputPath: " + inputPath)

    println("Loading input file")
    val rawDF = spark.read
      .format("csv")
      .option("header", "true") // first line in file has headers
      //.option("mode", "DROPMALFORMED")
      .option("inferSchema","true")
      .option("nullValue","defaultvalue")
      .load(inputPath)

    println(rawDF.columns.toSeq)
    val df = rawDF.na.fill(0)
    df.show(10)
    print("Hello!!")
  }
}

