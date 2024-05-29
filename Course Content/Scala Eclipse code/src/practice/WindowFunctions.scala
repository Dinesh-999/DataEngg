package practice

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object WindowFunctions extends App {

  val spark: SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("SparkByExamples.com")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  import spark.implicits._

  val simpleData = Seq(
    ("James", "Sales", 3000),
    ("Michael", "Sales", 4600),
    ("Robert", "Sales", 4100),
    ("Maria", "Finance", 3000),
    ("James", "Sales", 3000),
    ("Scott", "Finance", 3300),
    ("Jen", "Finance", 3900),
    ("Jeff", "Marketing", 3000),
    ("Kumar", "Marketing", 2000),
    ("Saif", "Sales", 4100))
  val df = simpleData.toDF("employee_name", "department", "salary")
  df.show()

  //row_number
  val windowSpec = Window.partitionBy("department").orderBy("salary")
//  val windowSpec = Window.orderBy("salary")
  df.withColumn("row_number", row_number.over(windowSpec))
    .show()

  //rank
  df.withColumn("rank", rank().over(windowSpec))
    .show()

  //dens_rank
  df.withColumn("dense_rank", dense_rank().over(windowSpec))
    .show()

  //percent_rank
  df.withColumn("percent_rank", percent_rank().over(windowSpec))
    .show()


  //lag
  df.withColumn("lag", lag("salary", 2).over(windowSpec))
    .show()

  //lead
  df.withColumn("lead", lead("salary", 2).over(windowSpec))
    .show()


}
