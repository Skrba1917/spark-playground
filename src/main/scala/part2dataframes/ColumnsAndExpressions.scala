package part2dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ColumnsAndExpressions extends App {

  val spark = SparkSession.builder()
    .appName("DFColumnsAndExpressions")
    .config("spark.master", "local")
    .getOrCreate()

  val carsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/cars.json")

  //carsDF.show()

  // Columns
  val firstColumn = carsDF.col("Name")

  // selecting (projecting)
  val carNamesDF = carsDF.select(firstColumn)
  //carNamesDF.show()

  // various select methods
  import spark.implicits._
  carsDF.select(
    carsDF.col("Name"),
    col("Acceleration"),
    column("Weight_in_lbs"),
    'Year, // scala symbol, auto-converted to column
    $"Horsepower", // fancier interpolated string, returns a Column object
    expr("Origin") // EXPRESSION
  )

  // select with plain column names
  carsDF.select("Name", "Year")

  // EXPRESSIONS
  val simplestExpression = carsDF.col("Weight_in_lbs")
  val weightInKgExpression = carsDF.col("Weight_in_lbs") / 2.2

  val carsWithWeightsDF = carsDF.select(
    col("Name"),
    col("Weight_in_lbs"),
    weightInKgExpression.as("Weight_in_kg"),
    expr("Weight_in_lbs / 2.2").as("Weight_in_kg_2")
  )
  //carsWithWeightsDF.show()

  val carsWithSelectExprWeightsDF = carsDF.selectExpr(
    "Name",
    "Weight_in_lbs",
    "Weight_in_lbs / 2.2"
  )

  // DF processing
  val carsWithKg3DF = carsDF.withColumn("Weight_in_kg_3", col("Weight_in_lbs") / 2.2)
  // renaming a column
  val carsWithColumnRenamedDF = carsDF.withColumnRenamed("Weight_in_lbs", "Weight in pounds")
  // be careful with column names
  carsWithColumnRenamedDF.selectExpr("`Weight in pounds`")
  // remove a column
  carsWithColumnRenamedDF.drop("Cylinders", "Displacement")

  // filtering
  val europeanCarsDF = carsDF.filter(col("Origin") =!= "USA")
  val europeanCarsDF2 = carsDF.where(col("Origin") =!= "USA")
  // filtering with expression strings
  val americanCarsDF = carsDF.filter("Origin = 'USA'")
  // chain filters
  val americanPowerfulCarsDF = carsDF.filter(col("Origin") === "USA")
    .filter(col("Horsepower") > 150)

  val americanPowerfulCarsDF2 = carsDF.filter((col("Origin") === "USA")
    .and(col("Horsepower") > 150))

  val americanPowerfulCarsDF3 = carsDF.filter("Origin = 'USA' and Horsepower > 150")

  // unioning = adding more rows
  val moreCarsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/more_cars.json")

  val allCarsDF = carsDF.union(moreCarsDF) // works if the DFs have the same schema

  // distinct values
  val allCountriesDF = carsDF.select("Origin").distinct()
  //allCountriesDF.show()

  /*
    First Exercise: read movies DF and select 2 columns of choice,
    create new column summing total profit of the movie,
    select all COMEDY movies with imdb rating above 6
   */

  // First Exercise Solution

  // read moviesDF
  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  // select 2 columns
  moviesDF.select("Title", "Distributor").show()

  // create new column with total profit
  val moviesProfitDF = moviesDF.select(
    col("US_Gross"),
    col("Worldwide_Gross"),
    col("US_DVD_Sales"),
    (col("US_Gross") + col("Worldwide_Gross").as("total_profit"))
  )

  // 2nd way to do it
  val moviesProfitDF2 = moviesDF.selectExpr(
    "US_Gross",
    "Worldwide_Gross",
    "US_Gross + Worldwide_Gross as total_profit"
  )

  // 3rd way to do it
  val moviesProfitDF3 = moviesDF.select("US_Gross", "Worldwide_Gross")
    .withColumn("tota_profit", col("US_Gross") + col("Worldwide_Gross"))

  //moviesProfitDF.show()
  //moviesProfitDF2.show()
  //moviesProfitDF3.show()

  // select comedies that are over 6
  val comediesDF = moviesDF.select("Title", "IMDB_Rating")
    .filter("Major_Genre = 'Comedy' and IMDB_Rating > 6")
  comediesDF.show()

  // there are many more ways to do this, this is just one way
}
