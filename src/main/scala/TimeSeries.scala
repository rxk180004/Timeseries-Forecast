import java.text.SimpleDateFormat
import java.time.format.DateTimeFormatter
import java.time.{LocalDateTime, ZoneId, ZonedDateTime}

import org.apache.spark.sql.functions._
import com.cloudera.sparkts.{BusinessDayFrequency, DateTimeIndex, DayFrequency, TimeSeriesRDD}
import com.cloudera.sparkts.models.ARIMA
import org.apache.spark.mllib.linalg.{DenseVector, Vector}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.joda.time.DateTime
import vegas._
import vegas.render.WindowRenderer._
import vegas.sparkExt._

object TimeSeries {

  def plotDF(df: DataFrame, title: String, symbol: String, xLabel: String, yLabel: String) = {

    Vegas("Time Series Line Chart", width=500.0, height=300.0)
      .withDataFrame(df)
      .mark(Line)
      //.encodeX("date", Temporal, timeUnit = TimeUnit.Yearmonth,
      //  axis = Axis(axisWidth = 0.0, format = "%Y", labelAngle = 0.0, tickSize = Some(0.0)),
      //  scale = Scale(nice = spec.Spec.NiceTimeEnums.Year)
      //)
      .encodeX(xLabel, Temp)
      .encodeY(yLabel, Quant)
      .encodeColor(
        field=symbol,
        dataType=Nominal,
        legend=Legend(orient="left", title="Stock Symbol"))
      .encodeDetailFields(Field(field=symbol, dataType=Nominal))
      .show
  }

  /**
    * Converts time series data format to observations format
    * Requires all columns that are getting converted to rows should be of same type
    * @param df
    * @param by (Column that needs to be left as is)
    * @param key
    * @param value
    * @return Modified DataFrame
    */
  def toObservationsFormat(df: DataFrame, by: Seq[String], key: String, value: String): DataFrame = {
    val (cols, types) = df.dtypes.filter{ case (c, _) => !by.contains(c)}.unzip
    require(types.distinct.size == 1, s"${types.distinct.toString}.length != 1")

    val kvs = explode(array(
      cols.map(c => struct(lit(c).alias(key), col(c).alias(value))): _*
    ))

    val byExprs = by.map(col(_))

    df
      .select(byExprs :+ kvs.alias("_kvs"): _*)
      .select(byExprs ++ Seq(col("_kvs." + key), col("_kvs." + value)): _*)
  }

  /** Makes sure only ERROR messages get logged to avoid log spam. */
  def setupLogging() = {
    import org.apache.log4j.{Level, Logger}
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)
  }

  def main(args: Array[String]): Unit = {
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

    val inputPath = "data/train_min.csv" // args(0)
    println("inputPath: " + inputPath)

    val pageCol = "page"
    val predictedCol = "prediction"
    val daysToForecast = 10
    val zoneId = ZoneId.of("Z")

    println("Loading input file")
    val rawDF = spark.read
      .format("csv")
      .option("header", "true") // first line in file has headers
      .option("inferSchema","true")
      .option("nullValue","defaultvalue")
      .load(inputPath)

    // Replace NULL and missing values with 0
    var df = rawDF.na.fill(0)

    // Get column names
    val colNames = rawDF.columns.toSeq

    //val validationColNames = colNames.slice(colNames.)

    df = df.drop(colNames.tail(0))

    df.show()

    // Get start and end dates to build timeseries RDD
    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm")
    val startDateStr = colNames(1) + " 00:00"
    val endDateStr = colNames(colNames.length - 1) + " 00:00"
    print(startDateStr, endDateStr)


    val startDate = LocalDateTime.parse(startDateStr, formatter)
    val endDate = LocalDateTime.parse(endDateStr, formatter)

    val trainUntil = endDate.minusDays(daysToForecast)

    val dateIndex = DateTimeIndex.uniformFromInterval(
      ZonedDateTime.of(startDate, ZoneId.of("Z")),
      ZonedDateTime.of(startDate, ZoneId.of("Z")),
      new DayFrequency(1)
    )

    val timeseriesRDD = df.map { line: Row =>
      val cols: Seq[Any] = line.toSeq
      val key = cols.head
      val hits = cols.tail.toArray
      val series = new DenseVector(hits.map(_.toString.toDouble))
      (key.toString, series.asInstanceOf[Vector])
    }.rdd

    val tsRDD: TimeSeriesRDD[String] = new TimeSeriesRDD[String](dateIndex, timeseriesRDD)

    // Find a sub-slice between two dates
    val subslice = tsRDD.slice(startDate.atZone(zoneId), trainUntil.atZone(zoneId))

    // Fill in missing values based on linear interpolation

    //val filledRDD = subslice.fill("linear")

    tsRDD.toDF().show(1)
    subslice.toDF().show(1)

    // filledRDD.toObservationsDataFrame

    print("Hello!!")

    val newDF = subslice.mapSeries { vector => {
      //val newVec = new DenseVector(vector.toArray.map(x => if (x.equals(Double.NaN)) 0 else x))
      val arimaModel = ARIMA.fitModel(1, 0, 0, vector)
      val forecasted = arimaModel.forecast(vector, daysToForecast)
      //println ("forecast", forecasted.size - daysToForecast, forecasted.size, forecasted.size - (daysToForecast + 1), forecasted.size - 1)
      new DenseVector(forecasted.toArray.slice(forecasted.size - (daysToForecast + 1), forecasted.size))
    }
    }.toDF(pageCol, predictedCol)

    newDF.show()

    //val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val simpleDateFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd")

    val forecastDates = (for (f <- 1 to daysToForecast) yield trainUntil.plusDays(f)).map(d => d.format(simpleDateFormat))
    //println("size!! ", forecastDates.size)
    var totalErrorSquare = 0.0

    val vecToSeq = udf((v: Vector) => v.toArray)

    val exprs = (0 until daysToForecast).map(i => ($""+ predictedCol).getItem(i).alias(s"exploded_col$i"))

    val getItem = udf((v: Vector, i: Int) => {
      print(v, i)
      v(i)
    })

    //val getItem = udf((v: Vector, i: Int) => v(i))

    val toArr: Any => Array[Int] = _.asInstanceOf[DenseVector].toArray.map(_.toInt)
    val toArrUdf = udf(toArr)
    var arrDF = newDF.withColumn("prediction_arr", toArrUdf('prediction))


    var fdf = forecastDates.zipWithIndex.foldLeft(arrDF) {
      (df, column) => {
        // println("fdf", column._1, column._2)
        df.withColumn(column._1, col("prediction_arr")(column._2))
      }
    }

    //fdf.show()

    fdf = fdf.drop("prediction").drop("prediction_arr")

    //fdf.show()

    //val df2 = newDF.withColumn(predictedCol, vecToSeq($"prediction"))
    //df2.show()
    //newDF.select($"page", split($"prediction")).show

    //for (i <- (predicted.size - period) until predicted.size) {
    //  val errorSquare = Math.pow(predicted(i) - amounts(i), 2)
    //  println(monthes(i) + ":\t\t" + predicted(i) + "\t should be \t" + amounts(i) + "\t Error Square = " + errorSquare)
    //  totalErrorSquare += errorSquare
    //}
    //println("Root Mean Square Error: " + Math.sqrt(totalErrorSquare/period))

    //print(newDF.select(predictedCol).collect().head.get(0).asInstanceOf[DenseVector].values)

    //(forecastDates zip newDF.select(predictedCol).collect().head.get(0).asInstanceOf[DenseVector].values)
    //  .map { case (date, hits) => (date, hits): (String, Double) }.toList

    println(forecastDates)

    //println("===========")
    //fdf.collect().map (row => {
    //  val page = row.getString(0)
    //  print("page", page)
    //  val value = row.getAs[Double](1)
    //  print("value", value)
    //})


    //val ttf = newDF.select(predictedCol).collect().head.get(0).asInstanceOf[DenseVector].values
    //  .map { case (hits) => (hits): (Double) }.toList

    //print(ttf)

    val toObsDF = toObservationsFormat(fdf, Seq(pageCol), "date", "views")
    toObsDF.show()
    plotDF(toObsDF, "fgfg", "page", "date", "views")

    val testData = "data/testData.csv"

    var oDF = spark.read
      .format("csv")
      .option("header", "true") // first line in file has headers
      .option("inferSchema","true")
      .option("nullValue","defaultvalue")
      .load(testData)

    //oDF = oDF.withColumn("open", 'open.cast("Int"))
    plotDF(oDF, "sfg", "symbol", "date", "open")
  }
}

