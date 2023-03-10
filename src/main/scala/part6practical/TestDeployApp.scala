package part6practical

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

object TestDeployApp {

  def main(args: Array[String]): Unit = {
     /*
        Movies.json as args(0)
        GoodComedies.json as args(1)
        good comedy = genre is Comedy and rating > 6.5
      */

    if (args.length != 2) {
      println("Need input and output path")
      System.exit(1)
    }

    val spark = SparkSession.builder()
      .appName("TestDeployApp")
      .getOrCreate()

    val moviesDF = spark.read
      .option("inferSchema", "true")
      .json(args(0))

    val goodComediesDF = moviesDF.select(
      col("Title"),
      col("IMDB_Rating").as("Rating"),
      col("Release_Date").as("Release")
    )
      .where(col("Major_Genre") === "Comedy" and col("IMDB_Rating") > 6.5)
      .orderBy(col("Rating").desc_nulls_last)

    goodComediesDF.show()

    goodComediesDF.write
      .mode(SaveMode.Overwrite)
      .format("json")
      .save(args(1))
  }

}
