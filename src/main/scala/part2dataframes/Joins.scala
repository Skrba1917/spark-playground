package part2dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Joins extends App {

  val spark = SparkSession.builder()
    .appName("Joins")
    .config("spark.master", "local")
    .getOrCreate()

  val guitarsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/guitars.json")

  val guitaristsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/guitarPlayers.json")

  val bandsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/bands.json")

  // joins
  // it's a good practice to extract join condition because we might need to reuse it often
  val joinCondition = guitaristsDF.col("band") === bandsDF.col("id")
  val guitaristsBandsDF = guitaristsDF.join(bandsDF, joinCondition, "inner")
  guitaristsBandsDF.show()

  // left outer join, contains everything in the inner join + all the rows in the LEFT table
  // with nulls where the data is missing
  // guitaristsDF is left table here and bandsDF is right
  guitaristsDF.join(bandsDF, joinCondition, "left_outer").show()

  // right outer join, same as left but reversed
  guitaristsDF.join(bandsDF, joinCondition, "right_outer").show()

  // outer join, inner join + all the rows in both tables
  guitaristsDF.join(bandsDF, joinCondition, "outer").show()

  // semi-joins, everything in the left DF for which there is a row in right DF satisfying the condition
  guitaristsDF.join(bandsDF, joinCondition, "left_semi").show()

  // anti-join, everything in the left DF for which there is NO row in right DF satisfying the condition
  guitaristsDF.join(bandsDF, joinCondition, "left_anti").show()

  // things to keep in mind when using joins
  // guitaristsBandsDF.select("id", "band").show() ---> this crashes because there are two id fields

  // option 1 - rename the column on which we are joining
  //guitaristsDF.join(bandsDF.withColumnRenamed("id", "band"), "band")

  // option 2 - drop the dupe column
  //guitaristsBandsDF.drop(bandsDF.col("id"))

  // option 3 - rename the offending column and keep the data
  //val bandsModDF = bandsDF.withColumnRenamed("id", "bandId")
  //guitaristsDF.join(bandsModDF, guitaristsDF.col("band") === bandsModDF.col("id"))

  // using complex types
  //guitaristsDF.join(guitaristsDF.withColumnRenamed("id", "guitarId"),
  //  expr("array_contains(guitars, guitarId)")
  //)

  /*
    First Exercise: show all employees and their max salary
    Second Exercise: show all employees who were never managers
    Third Exercise: find job titles of the best paid 10 employees from the company
   */

  def readTable(tableName: String) = spark.read
    .format("jdbc")
    .option("driver", "org.postgresql.Driver")
    .option("url", "jdbc:postgresql://localhost:5432/rtjvm")
    .option("user", "docker")
    .option("password", "docker")
    .option("dbtable", s"public.$tableName")
    .load()

  val employeesDF = readTable("employees")
  val salariesDF = readTable("salaries")
  val deptManagersDF = readTable("dept_manager")
  val titlesDF = readTable("titles")

  // First Exercise Solution
  val employeeMaxSalary = salariesDF.groupBy("emp_no").agg(max("salary").as("maxSalary"))
  val employeesSalariesDF = employeesDF.join(employeeMaxSalary, "emp_no")

  employeesSalariesDF.show()

  // Second Exercise Solution
  val employeeNotManagerDF = employeesDF.join(deptManagersDF,
    employeesDF.col("emp_no") === deptManagersDF.col("emp_no"),
    "left_anti"
  )

  employeeNotManagerDF.show()

  // Third Exercise Solution
  val mostRecentJobTitlesDF = titlesDF.groupBy("emp_no", "title").agg(max("to_date"))
  val mostPayedEmployeesDF = employeesSalariesDF.orderBy(col("maxSalary").desc).limit(10)
  val mostPayedJobsDF = mostPayedEmployeesDF.join(mostRecentJobTitlesDF, "emp_no")

  mostPayedJobsDF.show()

}
