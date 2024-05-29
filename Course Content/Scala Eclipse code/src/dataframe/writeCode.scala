package dataframe

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object writeCode {
  def main(args: Array[String]): Unit = {
    
    System.setProperty("hadoop.home.dir", "C:\\hadoop")
    // change your path accordingly for winutils
    println("====started==")
    
    val conf = new SparkConf().setAppName("first").setMaster("local[*]")
      .set("spark.driver.allowMultipleContexts", "true")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    
    val df = spark.read.format("csv")
      .option("header", "true")
      .load("file:///C:/data/usdata.csv")
    // change your path accordingly for source data
    df.show()
    
    val rowdf = df.withColumn("row", monotonically_increasing_id() + 1)
    rowdf.show()
    
    val filterdata = rowdf.filter(col("state") === "LA")
    filterdata.show()
    
    filterdata.write.format("parquet").partitionBy("county").mode("overwrite").save("file:///C:/data/usprocdir1")
  }

}
