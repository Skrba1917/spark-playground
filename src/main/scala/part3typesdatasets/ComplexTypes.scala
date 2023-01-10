package part3typesdatasets

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ComplexTypes extends App {

  val spark = SparkSession.builder()
    .appName("ComplexTypes")
    .config("spark.master", "local")
    .getOrCreate()

  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  // Dates

  import spark.implicits._

  spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
  val moviesWithReleaseDatesDF = moviesDF
    .select(col("Title"), to_date(col("Release_Date"), "dd-MMM-YY").as("Actual_Release"))

  moviesWithReleaseDatesDF
    .withColumn("Today", current_date())
    .withColumn("Right_Now", current_timestamp())
    .withColumn("Movie_Age", datediff($"Today", $"Actual_Release") / 365) // date_add, date_sub
    //.show()

  moviesWithReleaseDatesDF.select("*")
    .where($"Actual_Release".isNull)//.show()

  // First Exercise: read the stocks DF symbol column and parse the dates

  val stocksDF = spark.read
    .option("inferSchema", "true")
    .option("header", "true")
    .csv("src/main/resources/data/stocks.csv")

  stocksDF.withColumn("parsed_date", to_date(col("date"), "MMM dd yyyy"))
    //.show()

  /*
    If we have multiple date formats, we can parse the DF multiple times and then union the small DFs
    or just ignore bad formats if there aren't many of them
   */

  // Structures

  // 1 - with col operators
  moviesDF
    .select(col("Title"), struct(col("US_Gross"), col("Worldwide_Gross")).as("Profit"))
    .select(col("Title"), col("Profit").getField("US_Gross").as("US_Profit"))
    //.show()

  // 2 - with expression strings
  moviesDF.selectExpr("Title", "(US_Gross, Worldwide_Gross) as Profit")
    .selectExpr("Title", "Profit.US_Gross")
    //.show()

  // Arrays
  val moviesWithWordsDF = moviesDF
    .select(col("Title"), split(col("Title"), " |,").as("Title_Words")) // returns an array of strings

  moviesWithWordsDF.select(
    col("Title"),
    expr("Title_Words[0]"),
    size(col("Title_Words")),
    array_contains(col("Title_Words"), "Love")
  ).show()







}
