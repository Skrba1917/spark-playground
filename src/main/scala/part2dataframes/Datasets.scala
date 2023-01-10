package part2dataframes

import org.apache.spark.sql.{Dataset, Encoders, SparkSession}
import org.apache.spark.sql.functions._

object Datasets extends App {

  val spark = SparkSession.builder()
    .appName("Datasets")
    .config("spark.master", "local")
    .getOrCreate()

  val numbersDF = spark.read
    .option("inferSchema", "true")
    .option("header", "true")
    .csv("src/main/resources/data/numbers.csv")

  // numbersDF.show()
  // numbersDF.printSchema()

  // convert a DF to Dataset
  implicit val intEncoder = Encoders.scalaInt // encoder of int is capable of turning row in df into an int which is used in ds
  val numbersDS: Dataset[Int] = numbersDF.as[Int]

  numbersDS.filter(_ < 100)

  spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

  // dataset of a complex type
  // 1 - define your case class
  case class Car (
                   Name: String,
                   Miles_per_Gallon: Option[Double],
                   Cylinders: Long,
                   Displacement: Double,
                   Horsepower: Option[Long],
                   Weight_in_lbs: Long,
                   Acceleration: Double,
                   Year: String,
                   Origin: String
                 )

  // 2 - read the DF from the file
  def readDF(filename: String) = spark.read
    .option("inferSchema", "true")
    .json(s"src/main/resources/data/$filename")

  // 3 - define an encoder (importing the implicits)
  import spark.implicits._
  val carsDF = readDF("cars.json")
  // 4 - convert the DF to DS
  val carsDS = carsDF.as[Car]

  //carsDS.printSchema()
  //carsDS.show()

  // DS collection functions
  numbersDS.filter(_ < 100)//.show()

  // map, flatMap, fold, reduce...
  val carNamesDS = carsDS.map(car => car.Name.toUpperCase())
  //carNamesDS.show()

  // First Exercise: count how many cars we have
  // Second Exercise: count how many powerful cars (HP > 140)
  // Third Exercise: average HP for entire dataset

  // First Exercise Solution
  println("There are " + carsDS.count() + " cars.")

  // Second Exercise Solution
  println(carsDS.filter(_.Horsepower.getOrElse(0L) > 140).count())

  // Third Exercise Solution
  println(carsDS.map(_.Horsepower.getOrElse(0L)).reduce(_ + _) / carsDS.count())

  // alternative way for 3rd exercise
  carsDS.select(avg(col("Horsepower"))).show()

  // Joins
  case class Guitar(
                   id: Long,
                   make: String,
                   model: String,
                   guitarType: String
                   )

  case class GuitarPlayer (
                          id: Long,
                          name: String,
                          guitars: Seq[Long],
                          band: Long
                          )

  case class Band (
                  id: Long,
                  name: String,
                  hometown: String,
                  year: Long
                  )

  val guitarsDS = readDF("guitars.json").as[Guitar]
  val guitarPlayersDS = readDF("guitarPlayers.json").as[GuitarPlayer]
  val bandsDS = readDF("bands.json").as[Band]

  val guitarPlayerBandsDS: Dataset[(GuitarPlayer, Band)] = guitarPlayersDS.joinWith(bandsDS, guitarPlayersDS.col("band") === bandsDS.col("id"), "inner")
  guitarPlayerBandsDS.show()

  // First Exercise: join guitarsDS and guitarPlayersDS, in an outer join

  // First Exercise Solution
  val joined = guitarPlayersDS.joinWith(guitarsDS, array_contains(guitarPlayersDS.col("guitars"), guitarsDS.col("id")), "outer")
  joined.show()

  // grouping ds
  val carsGroupedByOrigin = carsDS
    .groupByKey(_.Origin)
    .count()
    .show()

  // joins and groups are WIDE transformations, will involve SHUFFLE operations



}
