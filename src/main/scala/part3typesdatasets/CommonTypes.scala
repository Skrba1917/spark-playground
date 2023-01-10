package part3typesdatasets

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object CommonTypes extends App {

  val spark = SparkSession.builder()
    .appName("CommonTypes")
    .config("spark.master", "local")
    .getOrCreate()

  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  // adding a plain value to DF
  moviesDF.select(col("Title"), lit(47).as("plain_value"))//.show

  // Booleans
  val dramaFilter = col("Major_Genre") === "Drama"
  val goodRatingFilter = col("IMDB_Rating") > 7.0
  val preferredFilter = dramaFilter and goodRatingFilter

  // multiple filters
  moviesDF.select("Title").where(dramaFilter)//.show

  val moviesWithGoodnessFlagsDF = moviesDF.select(col("Title"), preferredFilter.as("good_movie"))//.show()
  // filtering a boolean column
  // we can use not for negation
  moviesWithGoodnessFlagsDF.where("good_movie") // where(col("good_movie") === "true")

  // Numbers

  // math operators
  val moviesAvgRatingsDF = moviesDF.select(col("Title"),
    (col("Rotten_Tomatoes_Rating") / 10 + col("IMDB_Rating")) / 2)//.show()

  // correlation = number between -1 and 1
  println(moviesDF.stat.corr("Rotten_Tomatoes_Rating", "IMDB_Rating")) // corr is an ACTION

  // Strings
  val carsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/cars.json")

  import spark.implicits._

  // capitalizes first letter of every word. We also have upper, lower
  carsDF.select(initcap($"Name"))//.show

  // contains
  carsDF.select("*").where($"Name".contains("vw"))//.show()

  // regex
  val regexString = "volkswagen|vw" // | is or operator
  val vwDF = carsDF.select(
    col("Name"),
    regexp_extract(col("Name"), regexString, 0).as("regex_extract")
  ).where(col("regex_extract") =!= "")//.show()

  vwDF.select(
    col("Name"),
    regexp_replace(col("Name"), regexString, "People's Car").as("regex_replace")
  )//.show()

  // First Exercise: filter the cars df by a list of car names obtained by an API call

  def getCarNames: List[String] = List("Volkswagen", "Mercedes-Benz", "Ford")

  val regexCars = getCarNames.map(_.toLowerCase()).mkString("|") // makes the list lower case and split with regex |

  carsDF.select(
    col("Name"),
    regexp_extract(col("Name"), regexCars, 0).as("filtered_cars")
  ).where(col("filtered_cars") =!= "").drop("filtered_cars")
    .show()

  // version 2 - contains
  val carNameFilters = getCarNames.map(_.toLowerCase())
    .map(name => col("Name").contains(name))

  val bigFilter = carNameFilters.fold(lit(false))((combinedFilter, newCarNameFilter) => combinedFilter or newCarNameFilter)
  carsDF.filter(bigFilter).show()

}
