import java.time.format.DateTimeFormatter
import java.time.{LocalDateTime, ZoneId, ZonedDateTime}
import java.util.Random

import org.apache.spark.sql.functions._
import com.cloudera.sparkts.{DateTimeIndex, DayFrequency, TimeSeriesRDD}
import com.cloudera.sparkts.models.ARIMA
import org.apache.spark.mllib.linalg.{DenseVector, Vector}
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}
import vegas._
import vegas.render.WindowRenderer._
import vegas.sparkExt._

object TimeSeries {

  /**
    * Sums given columns in a dataframe row wise
    * @param cols
    * @return
    */
  def sum(cols: Column*) = cols.foldLeft(lit(0))(_ + _)

  /**
    * Plots timeseries data using Vegas library
    * @param df
    * @param title
    * @param symbol
    * @param xLabel
    * @param yLabel
    */
  def plotDF(df: DataFrame, title: String, symbol: String, xLabel: String, yLabel: String) = {

    Vegas("Time Series Line Chart", width=500.0, height=300.0)
      .withDataFrame(df)
      .mark(Line)
      .encodeX(xLabel, Temp)
      .encodeY(yLabel, Quant)
      .encodeColor(
        field=symbol,
        dataType=Nominal,
        legend=Legend(orient="left", title="Legend"))
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

  /**
    * Makes sure only ERROR messages get logged to avoid log spam.
    */
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

    if (args.length < 1) {
      System.err.println("Correct usage: Program_Name inputPath")
      System.exit(1)
    }

    // Get rid of log spam (should be called after the context is set up)
    setupLogging()

    val inputPath = args(0)
    println("inputPath: " + inputPath)

    val pageCol = "page"
    val predictedCol = "prediction"
    val typeCol = "type"
    val totalCol = "total"
    val daysToForecast = 10
    val zoneId = ZoneId.of("Z")

    println("Loading input file")
    val rawDF = spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema","true")
      .option("nullValue","defaultvalue")
      .load(inputPath)

    // Replace NULL and missing values with 0
    var df = rawDF.na.fill(0)

    // Get column names
    val colNames = rawDF.columns.toSeq

    df = df.drop(colNames.tail(0))

    val dates = df.columns.tail

    val columnsToSum = dates.map(col _).toList

    // Calculate total hits for each page and store it as sum
    df = df.withColumn(totalCol, columnsToSum.reduce(_ + _))

    //df.sort(col(totalCol).desc).show()

    // Get top 10 pages
    var topPagesList = df.sort(col(totalCol).desc).take(100).map(x => x.getString(0))

    df = df.filter(col(pageCol).isin(topPagesList : _*))

    // Get start and end dates to build timeseries RDD
    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm")
    val startDateStr = colNames(1) + " 00:00"
    val endDateStr = colNames(colNames.length - 1) + " 00:00"


    val startDate = LocalDateTime.parse(startDateStr, formatter)
    val endDate = LocalDateTime.parse(endDateStr, formatter)

    // train until this date
    val trainUntil = endDate.minusDays(daysToForecast)

    // train
    val dateIndex = DateTimeIndex.uniformFromInterval(
      ZonedDateTime.of(startDate, ZoneId.of("Z")),
      ZonedDateTime.of(startDate, ZoneId.of("Z")),
      new DayFrequency(1)
    )

    // Build rdd in a way that spark-ts library accepts
    // Format (String, Vector[])
    // String has key and Vector holds timeseries information
    val timeseriesRDD = df.map { line: Row =>
      val cols: Seq[Any] = line.toSeq
      // Head has page title
      val key = cols.head
      // Tail has all date columns
      val hits = cols.tail.toArray
      val series = new DenseVector(hits.map(_.toString.toDouble))
      (key.toString, series.asInstanceOf[Vector])
    }.rdd

    // Once the rdd is built feed it to library by passing build the DF
    val tsRDD: TimeSeriesRDD[String] = new TimeSeriesRDD[String](dateIndex, timeseriesRDD)

    // Find a sub-slice between two dates
    val subslice = tsRDD.slice(startDate.atZone(zoneId), trainUntil.atZone(zoneId))

    // Fill in missing values based on linear interpolation
    val filledRDD = subslice.fill("linear")

    val timeseriesDF = subslice.mapSeries { vector => {
      val newVec = new DenseVector(vector.toArray.map(x => if (x.equals(Double.NaN)) 0 else x))
      val arimaModel = ARIMA.fitModel(1, 0, 1, newVec)
      //val arimaModel = ARIMA.autoFit(newVec)
      val forecasted = arimaModel.forecast(newVec, daysToForecast)

      new DenseVector(forecasted.toArray.slice(forecasted.size - (daysToForecast + 1), forecasted.size))
    }
    }.toDF(pageCol, predictedCol)

    //timeseriesDF.show()

    val simpleDateFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd")

    // Build array of forecasted dates that we use to explode DenseVector to columns
    val forecastDates = (for (f <- 1 to daysToForecast) yield trainUntil.plusDays(f)).map(d => d.format(simpleDateFormat))

    var totalErrorSquare = 0.0

    val vecToSeq = udf((v: Vector) => v.toArray)

    val exprs = (0 until daysToForecast).map(i => ($""+ predictedCol).getItem(i).alias(s"exploded_col$i"))

    val getItem = udf((v: Vector, i: Int) => v(i))

    // Converting dense vector into an array and type casting it to integers
    val toArr: Any => Array[Int] = _.asInstanceOf[DenseVector].toArray.map(_.toInt)
    val toArrUdf = udf(toArr)
    var arrDF = timeseriesDF.withColumn("prediction_arr", toArrUdf('prediction))


    // Build dataframe using the above generated dates by zipping it with the obtained dataframe
    // fdf - forecasted df
    var fdf = forecastDates.zipWithIndex.foldLeft(arrDF) {
      (df, column) => {
        df.withColumn(column._1, col("prediction_arr")(column._2))
      }
    }

    fdf.show()

    // as we are now done with prediction and prediction_arr columns, let drop them
    fdf = fdf.drop("prediction").drop("prediction_arr")

    println("forecated dates" ,forecastDates)

    // Now we got forecasted results and we already had actual values to those dates.
    // So now lets combine those two dataframes together using UNION method.
    // As it will be difficult to identify rows from forecasted and acutal dataframes we are introducing a new column
    // to identity them while plotting the graph (actual/forecated)
    // Below code adds type column to dataframe
    val actualDF = df.select(pageCol, forecastDates: _*).withColumn("type", lit("actual"))
    fdf = fdf.withColumn("type", lit("forecasted"))

    //fdf.show()

    // Join both dataframes using UNION
    val combinedDFs = Seq(actualDF, fdf).reduce(_ union _) //actualDF union fdf

    //combinedDFs.show(200)

    // Now convert the resultant dataframe in timeseries to observations format
    // We are doing this only for the sake of plotting the data onto graphs using Vegas as they don't support timeseries
    // format
    val combinedDFObservationFormat = toObservationsFormat(combinedDFs, Seq(pageCol, typeCol), "date", "views")

    // Plot top 3 pages data to see our forecasting/predictions
    val sampleSize = 3
    var index = 0

    while (index < sampleSize) {
      val selectedPage = topPagesList(index)
      println (selectedPage)
      // filter the page for the given title
      var filteredDF = combinedDFObservationFormat.filter(col(pageCol) === selectedPage)
      //filteredDF.show()
      filteredDF = filteredDF.drop(pageCol)
      plotDF(filteredDF, selectedPage, typeCol, "date", "views")
      index = index + 1
    }
  }
}

