package dataframe

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object aggregation {

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\hadoop")
    
    println("====started==")
    val conf = new SparkConf().setAppName("first").setMaster("local[*]")
      .set("spark.driver.allowMultipleContexts", "true")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    
    val df = spark.read.format("csv")
      .option("header", "true")
      .load("file:///C:/data/agg1.csv")
    df.show()
    
    val aggdf = df.groupBy("name", "product")
      .agg(
        sum("amt").cast(IntegerType).as("total"),
        count("amt").as("cnt"))
      .orderBy(col("total") desc)
    aggdf.show()
    
    df.createOrReplaceTempView("df")
    
    val finald = spark.sql("select name,product,cast(sum(amt) as int) as total, count(amt) as cnt from df group by name,product order by total")
    finald.show()
  }

}