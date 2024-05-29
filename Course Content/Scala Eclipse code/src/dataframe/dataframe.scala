package dataframe

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object dataframe {

  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "C:\\hadoop")
    println("====started==")

    val conf = new SparkConf().setAppName("first").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    val df = spark.read.format("csv")
      .option("header", "true")
      .load("file:///D:/data/usdata.csv") // YOUR PATH HERE

    df.show()

    df.createOrReplaceTempView("ustab")

    val finaldf = spark.sql(" select * from ustab where state='LA' ")
    finaldf.show()

    // =========================================================================

    val csvdf = spark
      .read
      .format("csv")
      .option("header", "true")
      .load("file:///C:/data/sedata/usdata.csv")
    csvdf.show()

    val parquetdf = spark
      .read
      .format("parquet")
      .option("header", "true")
      .load("file:///C:/data/sedata/parquetdata.parquet")
    parquetdf.show()

    val jsonsdf = spark
      .read
      .format("json")
      .load("file:///C:/data/sedata/devices.json")
    jsonsdf.show()

    val orcdf = spark
      .read
      .format("orc")
      .load("file:///C:/data/sedata/part.orc")
    orcdf.show()

    //    ==============================================================================

    val dff = spark
      .read
      .format("csv")
      .option("header", "true")
      .load("file:///D:/data/dt.txt")
    dff.show()
    val filterdf = dff.filter(col("category") === "Exercise")
    filterdf.show()

    val df1 = dff.select("tdate", "category")
    df1.show()
    val df2 = dff.drop("tdate", "category")
    df2.show()

    //    ===================================================

    // Multi Column filter and
    val Multidf = df.filter(col("category") === "Exercise"
      && //and operator
      col("spendby") === "cash")
    Multidf.show()
    // Multi Column filter or
    val Multifilterdf = df.filter(col("category") === "Exercise"
      || //or operator
      col("spendby") === "cash")
    Multifilterdf.show()

    // Multi value filter
    val isindf = df.filter(col("category") isin ("Exercise", "Team Sports"))
    isindf.show()

  }

}