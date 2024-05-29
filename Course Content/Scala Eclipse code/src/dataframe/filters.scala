package dataframe

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object filters {

  //  category = Exercise
  //  category = Exercise && spendby=cash
  //  category = Exercise or spendby=cash
  //  category = Exercise, Team Sports (both)
  //  product like %Gymnastics%
  //  category != Exercise && spendby=cash
  //  product is null
  //  product is not null

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\hadoop")
    println("====started==")
    val conf = new SparkConf().setAppName("first").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    val df = spark.read
      .format("csv")
      .option("header", "true")
      .load("file:///D:/data/dt.txt")
    df.show()

    println
    println("======one column filter=====")
    println
    val df1 = df.filter(col("category") === "Exercise")
    df1.show()

    println
    println("======Multi column filter=====")
    println
    val df2 = df.filter(col("category") === "Exercise" && col("spendby") === "cash")
    df2.show()

    println
    println("======Multi or filter=====")
    println
    val df3 = df.filter(col("category") === "Exercise" || col("spendby") === "cash")
    df3.show()

    println
    println("======Multi value=====")
    println
    val df4 = df.filter(col("category") isin ("Exercise", "Team Sports"))
    df4.show()

    println
    println("======like filter=====")
    println
    val df5 = df.filter(col("product") like "%Gymnastics%")
    df5.show()

    println
    println("======Not filter=====")
    println
    val df6 = df.filter(!(col("category") === "Exercise") && col("spendby") === "cash")
    df6.show()

    println
    println("======null filter=====")
    println
    val df7 = df.filter(col("product") isNull)
    df7.show()

    println
    println("======Not null filter=====")
    println
    val df8 = df.filter(col("product") isNotNull)
    df8.show()

    val sqldf2 = df.selectExpr("*", "case when spendby='cash' then 1 else 0 end as status")
    sqldf2.show()

    val sqldf3 = df.selectExpr("id", "product", "lower(category) as lower")
    sqldf3.show()

    val sqldf4 = df.selectExpr(
      "id",
      "split(tdate,'-')[2] as year",
      "amount",
      "category",
      "product",
      "spendby")
    df1.show()

    val withcolumndf = df.withColumn("tdate", expr("split(tdate,'-')[2]")).withColumnRenamed("tdate", "year")
    withcolumndf.show()

    //Task 1
    val withcoldf1 = df.withColumn("tdate1", expr("split(tdate,'-')[2]"))
      .withColumnRenamed("tdate", "year")
    withcoldf1.show()
    
    val withcoldf2 = spark.read
      .format("csv")
      .load("file:///D:/data/datatxns.txt") // your path
    withcoldf2.show()
    
    //Task 2
    val withcoldf3 = df2.selectExpr(
      "*", "case when _c2 like '%Gymnastics%' then 'yes' else 'no' end as status")
    withcoldf3.show()
    
    val withcolexpr = df.withColumn("category",expr("upper(category)"))
            .withColumn("status", expr("case when spendby='cash' then 1 else 0 end"))
    withcolexpr.show()

  }

}