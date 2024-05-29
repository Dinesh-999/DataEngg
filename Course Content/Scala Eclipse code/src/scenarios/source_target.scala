package scenarios

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object source_target {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\hadoop")
    println("====started==")
    val conf = new SparkConf().setAppName("first").setMaster("local[*]")
      .set("spark.driver.allowMultipleContexts", "true")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    
    val source = spark.read.format("csv").option("header", "true").load("file:///C:/data/source.csv")
    source.show()
    
    val target = spark.read.format("csv").option("header", "true").load("file:///C:/data/target.csv").withColumnRenamed("name", "name1")
    target.show()
    
    val full = source.join(target, Seq("id"), "full").orderBy("id")
    full.show()
    
    val match_mis = full.withColumn("comment", expr("case when name=name1then 'Match' else 'Mismatch' end"))
    match_mis.show()
    
    val remvmatch = match_mis.filter(!(col("comment") === "Match"))
    remvmatch.show()
    
    val finaldfpre = remvmatch.withColumn("comment", expr("case when name1 isnull then 'New in Source' when name is null then 'New in Target' else commentend"))
    finaldfpre.show()
    
    val finaldf = finaldfpre.drop("name", "name1")
    finaldf.show()
  }

}