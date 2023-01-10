package part5lowlevel

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

import scala.io.Source

object RDDs extends App {

  val spark = SparkSession.builder()
    .appName("RDDs")
    .config("spark.master", "local")
    .getOrCreate()

  val sc = spark.sparkContext

  // 1 - parallelize an existing collection
  val numbers = 1 to 1000000
  val numbersRDD = sc.parallelize(numbers)

  // 2 - reading from files
  case class StockValue(symbol: String, date: String, price: Double)
  def readStocks(filename: String) =
    Source.fromFile(filename).getLines().drop(1).map(line => line.split(","))
      .map(tokens => StockValue(tokens(0), tokens(1), tokens(2).toDouble))
      .toList

  val stocksRDD = sc.parallelize(readStocks("src/main/resources/data/stocks.csv"))

  // 2b - reading from files alternative
  val stocksRDD2 = sc.textFile("src/main/resources/data/stocks.csv")
    .map(line => line.split(","))
    .filter(tokens => tokens(0).toUpperCase() == tokens(0))
    .map(tokens => StockValue(tokens(0), tokens(1), tokens(2).toDouble))

  // 3 - read from a DF
  val stocksDF = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("src/main/resources/data/stocks.csv")

  import spark.implicits._

  val stocksDS = stocksDF.as[StockValue]
  val stocksRDD3 = stocksDS.rdd

  // RDD to DF
  val numbersDF = numbersRDD.toDF("numbers") // we lose type information

  // RDD to DS
  val numbersDS = spark.createDataset(numbersRDD) // we keep type information

  // transformations
  val msftRDD = stocksRDD.filter(_.symbol == "MSFT") // lazy transformation

  // counting
  val msCount = msftRDD.count() // eager ACTION
  // distinct
  val companyNamesRDD = stocksRDD.map(_.symbol).distinct() // distinct also lazy

  // min max
  implicit val stockOrdering: Ordering[StockValue] = Ordering.fromLessThan((sa, sb) => sa.price < sb.price)
  val minMsft = msftRDD.min() // action

  // reducing
  numbersRDD.reduce(_ + _)

  // grouping
  val groupedStocksRDD = stocksRDD.groupBy(_.symbol)
  // grouping is very expensive

  // partitioning
  val repartitionedStocksRDD = stocksRDD.repartition(30)
  repartitionedStocksRDD.toDF.write
    .mode(SaveMode.Overwrite)
    .parquet("src/main/resources/data/stocks30")

  // repartitioning is also expensive because it involves shuffling
  // partition early, then process that -> best practice
  // size of partition 10-100MB

  // coalesce
  val coalesceRDD = repartitionedStocksRDD.coalesce(15) // does NOT involve shuffling
  coalesceRDD.toDF.write
    .mode(SaveMode.Overwrite)
    .parquet("src/main/resources/data/stocks15")

  // 1 - read the movies.json as an RDD
  case class Movie(title: String, genre: String, rating: Double)

  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  val moviesRDD = moviesDF.select(
    col("Title").as("title"),
    column("Major_Genre").as("genre"),
    column("IMDB_Rating").as("rating")
  ).where(
    col("genre").isNotNull and col("rating").isNotNull
  ).as[Movie]
    .rdd

  moviesRDD.toDF.show()

  // 2 - show the distinct genres as an RDD
  val genresRDD = moviesRDD.map(_.genre).distinct()

  genresRDD.toDF.show()

  // 3 - select all movies in the Drama genre rating > 6
  val goodDramasRDD = moviesRDD.filter(movie => movie.genre == "Drama" && movie.rating > 6)

  goodDramasRDD.toDF.show()

  // 4 - show the average rating of movies by genre
  case class GenreAverageRating(genre: String, rating: Double)
  val averageRatingByGenreRDD = moviesRDD.groupBy(_.genre).map {
    case (genre, movies) => GenreAverageRating(genre, movies.map(_.rating).sum / movies.size)
  }

  averageRatingByGenreRDD.toDF.show()
  moviesRDD.toDF.groupBy(col("genre")).avg("rating").show()       // with regular DF just for comparison

}
