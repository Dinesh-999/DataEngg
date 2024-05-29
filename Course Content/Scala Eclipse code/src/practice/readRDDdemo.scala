package practice

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row

object readRDDdemo {
  //  "txnno","txndate","custno","amount","category","product","city","state","spendby"
  case class dataSchema(txnno: String, txndate: String, custno: String, amount: String, category: String, product: String, city: String, state: String, spendby: String)
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("first").setMaster("local[*]").set("spark.driver.allowMultipleContexts", "true")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    import spark.implicits._

    val data = sc.textFile("file:///D:/Data Analytics applications/Eclipse Projects/data_files/file1.txt")
    data.take(5).foreach(println)

    println()

    val splitdata = data.map(x => x.split(","))
    splitdata.take(5).foreach(println)
    println()

    val schemais = StructType(Array(
      StructField("txnno", StringType, true),
      StructField("txndate", StringType, true),
      StructField("custno", StringType, true),
      StructField("amount", StringType, true),
      StructField("category", StringType, true),
      StructField("product", StringType, true),
      StructField("city", StringType, true),
      StructField("state", StringType, true),
      StructField("spendby", StringType, true)))

    val structdata = splitdata.map(x => Row(x(0), x(1), x(2), x(3), x(4), x(5), x(6), x(7), x(8)))
    
    val structdf = spark.createDataFrame(structdata, schemais)
    structdf.show(5)

    //    val schema_data = splitdata.map(x => dataSchema(x(0),x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8)))
    //    schema_data.take(5).foreach(println)
    //    println()
    //
    //    val df = schema_data.toDF()
    //    df.show()
    //    df.printSchema()

  }
}