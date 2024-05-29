package dataframe

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object joinFull {
  def main(args: Array[String]): Unit = {
    
    System.setProperty("hadoop.home.dir", "C:\\hadoop")
    println("====started==")
    
    val conf = new SparkConf().setAppName("first").setMaster("local[*]")
      .set("spark.driver.allowMultipleContexts", "true")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    
    val cust = spark.read.format("csv").option("header", "true").load("file:///C:/data/cust.csv")
    cust.show()
    
    val prod = spark.read.format("csv")
      .option("header", "true")
      .load("file:///C:/data/prod.csv")
    prod.show()
    
    println
    println("======inner join=======")
    println
    val inner = cust.join(prod, Seq("id"), "inner")
    inner.show()
    
    println
    println("======left join=======")
    println
    val left = cust.join(prod, Seq("id"), "left")
    left.show()
    
    println
    println("======right join=======")
    println
    val right = cust.join(prod, Seq("id"), "right")
    right.show()
    
    println
    println("======full join=======")
    println
    val full = cust.join(prod, Seq("id"), "full")
    full.show()
    
    println
    println("======left anti join=======")
    println
    val left_anti = cust.join(prod, Seq("id"), "left_anti")
    left_anti.show()
    
    println
    println("======cross join=======")
    println
    val cross = cust.crossJoin(prod)
    cross.show()
    
  }

}