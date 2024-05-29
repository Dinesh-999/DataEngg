package dataframe

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object AWSs3Integration {
  def main(args: Array[String]): Unit = {
    
    System.setProperty("hadoop.home.dir", "C:\\hadoop")
    println("====started==")
    
    val conf = new SparkConf().setAppName("first").setMaster("local[*]")
      .set("spark.driver.allowMultipleContexts", "true")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    
    val df = spark
      .read
      .format("json")
      .option("fs.s3a.access.key", "AKIAS3H27Y6URIBF3P4T")
      .option("fs.s3a.secret.key", "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")
      .load("s3a://liyabuck/devices.json")
    df.show()
  }

}