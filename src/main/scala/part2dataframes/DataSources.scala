package part2dataframes

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types._

object DataSources extends App {

  val spark = SparkSession.builder()
    .appName("DataSources")
    .config("spark.master", "local")
    .getOrCreate()

  val carsSchema = StructType(Array(
    StructField("Name", StringType),
    StructField("Miles_per_Gallon", DoubleType),
    StructField("Cylinders", LongType),
    StructField("Displacement", DoubleType),
    StructField("Horsepower", LongType),
    StructField("Weight_in_lbs", LongType),
    StructField("Acceleration", DoubleType),
    StructField("Year", StringType),
    StructField("Origin", StringType)
  ))

  val carsSchemaWithDateType = StructType(Array(
    StructField("Name", StringType),
    StructField("Miles_per_Gallon", DoubleType),
    StructField("Cylinders", LongType),
    StructField("Displacement", DoubleType),
    StructField("Horsepower", LongType),
    StructField("Weight_in_lbs", LongType),
    StructField("Acceleration", DoubleType),
    StructField("Year", DateType),
    StructField("Origin", StringType)
  ))


  val carsDF = spark.read
    .format("json")
    .schema(carsSchema)
    // failFast will drop exception if we edit .json file and set for e.g string instead of integer
    .option("mode", "failFast") // dropMalformed(ignore faulty rows), permissive(default), failFast drops exception
    .option("path", "src/main/resources/data/cars.json" )
    .load()

  // alternative read with options map
  val carsDFWithOptionMap = spark.read
    .format("json")
    .options(Map(
      "mode" -> "failFast",
      "path" -> "src/main/resources/data/cars.json",
      "inferSchema" -> "true"
    ))
    .load()

  // Writing DFs requires: format, save mode (overwrite, append, ignore, errorIfExists), path, zero or more options
  carsDF.write
    .format("json")
    .mode(SaveMode.Overwrite)
    .option("path", "src/main/resources/data/cars_dupe.json")
    .save() // or we can pass a path to the save method

  // JSON flags
  spark.read
    //.format("json")
    .schema(carsSchemaWithDateType)
    .option("dateFormat", "YYYY-MM-dd") // couple with schema; if Spark fails parsing, it will put null
    .option("allowSingleQuotes", "true")
    .option("compression", "uncompressed") // bzip2, gzip, lz4, snappy, deflate. Uncompressed is default
    .json("src/main/resources/data/cars.json") // we use this instead of format and load
    .show()

  // CSV flags
  val stocksSchema = StructType(
    Array(
      StructField("symbol", StringType),
      StructField("date", DateType),
      StructField("price", DoubleType)
    )
  )

  // We need spark timeParser LEGACY in order to parse the formats from this stocks.csv file
  spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
  spark.read
    //.format("csv")
    .schema(stocksSchema)
    .option("dateFormat", "MMM d YYYY")
    .option("header", "true")
    .option("sep", ",")
    .option("nullValue", "")
    .csv("src/main/resources/data/stocks.csv") // just like with json above, we use .csv instead of format and load
    .show()

  // Parquet
  carsDF.write
    .mode(SaveMode.Overwrite)
    .save("src/main/resources/data/cars.parquet") // parquet is the default format so we can use save

  // Text files
  spark.read.text("src/main/resources/data/sampleTextFile.txt")
    .show()

  // Reading from remote DB
  val employeesDF = spark.read
    .format("jdbc")
    .option("driver", "org.postgresql.Driver")
    .option("url", "jdbc:postgresql://localhost:5432/rtjvm")
    .option("user", "docker")
    .option("password", "docker")
    .option("dbtable", "public.employees")
    .load()

  // employeesDF.show()

  /*
    First Exercise: read the movies DF,
    write it as tab-separated values file, as snappy Parquet,
    as table in PostgresDB in "public.movies"

  */

  // First Exercise Solution

  val moviesDF = spark.read
    .format("json")
    .option("inferSchema", "true")
    .load("src/main/resources/data/movies.json")

  moviesDF.write
    .format("csv")
    .option("header", "true")
    .mode(SaveMode.Overwrite)
    .option("sep", "|")
    .save("src/main/resources/data/writtenmovies.csv")

  moviesDF.write
    .mode(SaveMode.Overwrite)
    .save("src/main/resources/data/writtenmovies.parquet")

  moviesDF.write
    .format("jdbc")
    .option("driver", "org.postgresql.Driver")
    .option("url", "jdbc:postgresql://localhost:5432/rtjvm")
    .option("user", "docker")
    .option("password", "docker")
    .option("dbtable", "public.movies")
    .save()



}
