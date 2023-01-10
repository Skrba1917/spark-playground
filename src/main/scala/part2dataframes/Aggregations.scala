package part2dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Aggregations extends App {

  val spark = SparkSession.builder()
    .appName("AggregationsAndGrouping")
    .config("spark.master", "local")
    .getOrCreate()

  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  // counting
  val genresCountDF = moviesDF.select(
    count(col("Major_Genre")) // all values except null
  )
  moviesDF.selectExpr("count(Major_Genre)")
  moviesDF.select(count("*")) // counts all the rows, including null
  //genresCountDF.show()

  // counting distinct
  moviesDF.select(countDistinct(col("Major_Genre")))
    //.show()

  // approximate count
  moviesDF.select(approx_count_distinct(col("Major_Genre")))
    //.show()

  // min and max
  val minRatingDF = moviesDF.select(min(col("IMDB_Rating")))
  val maxRatingDF = moviesDF.select(max(col("IMDB_Rating")))
  moviesDF.selectExpr("min(IMDB_Rating)")

  // sum
  moviesDF.select(sum(col("US_Gross")))
    //.show()
  moviesDF.selectExpr("sum(US_Gross)")
    //.show()

  // avg
  moviesDF.select(avg(col("Rotten_Tomatoes_Rating")))
    //.show()
  moviesDF.selectExpr("avg(Rotten_Tomatoes_Rating)")
    //.show()

  // data science
  moviesDF.select(
    mean(col("Rotten_Tomatoes_Rating")), // basically average
    stddev(col("Rotten_Tomatoes_Rating")) // standard deviation (how close/far the different values are to mean)
  )//.show()

  // Grouping
  val countByGenreDF = moviesDF
    .groupBy(col("Major_Genre")) // it includes null
    .count() // select count(*) from moviesDF group by Major_Genre in SQL

  // countByGenreDF.show()

  val avgRatingByGenreDF = moviesDF
    .groupBy(col("Major_Genre"))
    .avg("IMDB_Rating")

  // avgRatingByGenreDF.show()

  val aggregationsByGenre = moviesDF
    .groupBy(col("Major_Genre"))
    .agg(
      count("*").as("N_Movies"),
      avg("IMDB_Rating").as("Avg_Rating")
    ).orderBy(
    col("Avg_Rating")
    )

  //aggregationsByGenre.show()

  /*
    First Exercise: sum up all the profits of all the movies in the DF
    Second Exercise: count distinct directors
    Third Exercise: show mean and standard deviation of US Gross revenue
    Fourth Exercise: compute the average imdb rating and the sum of us gross revenue per director
   */

  // First Exercise Solution
  val totalProfitDF = moviesDF.selectExpr("sum(US_Gross + Worldwide_Gross + US_DVD_Sales) as total_profit_of_all_movies")
    .show()

  // Second Exercise Solution
  val distinctDirectorsDF = moviesDF.select(countDistinct(col("Director"))).show()

  // Third Exercise Solution
  val revenueResultsDF = moviesDF.select(
    mean(col("US_Gross")).as("mean_result"),
    stddev(col("US_Gross")).as("standard_deviation")
  ).show()

  // Fourth Exercise Solution
  val avgThingsPerDirector = moviesDF
    .groupBy("Director")
    .agg(
      avg("IMDB_Rating").as("Average_Imdb_Rating"),
      sum("US_Gross").as("Total_US_Gross")
    ).orderBy(col("Average_Imdb_Rating").desc_nulls_last)
    .show()



}
