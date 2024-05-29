package rddCode

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object rddBasicCode {

  case class schema(id: String, category: String, product: String, mode: String)

  def main(args: Array[String]): Unit = {
    println("===started Zeyo====")
    val conf = new SparkConf().setAppName("first").setMaster("local[*]").set("spark.driver.allowMultipleContexts", "true")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val data = sc.textFile("file:///C:/Users/dines/Downloads/data analytics/data/datatxns.txt")
    // ==> Change path according to your local file
//    data.foreach(println)
    data.take(5).foreach(println)

    //    ===========================================

    val usdata = sc.textFile("file:///C:/Users/dines/Downloads/data analytics/data/usdata.csv")
    usdata.foreach(println)
    val lendata = usdata.filter(x => x.length > 200)

    println
    println("======len data====")
    lendata.foreach(println)
    println

    println("======flatten data====")
    val flatten = lendata.flatMap(x => x.split(","))
    flatten.foreach(println)

    // ====================================================
    // Schema RDD

    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    println
    println("Schema RDD")
    println
    val datatxn = sc.textFile("file:///C:/Users/dines/Downloads/data analytics/data/datatxns.txt")
    datatxn.foreach(println)
    println
    println
    val mapsplit = datatxn.map(x => x.split(","))
    // search line 11 for schema
    val schemardd = mapsplit.map(x => schema(x(0), x(1), x(2), x(3)))
    
    println
    println("schema print")
    schemardd.foreach(println)
    
    println
    println
    val schemadf = schemardd.toDF()
    schemadf.show()
    
    val prodfilter = schemardd.filter(x => x.product.contains("Gymnastics"))
    prodfilter.foreach(println)
    println
    println
    val dataframe = prodfilter.toDF()
    dataframe.show()

    //    ======================================================
    //    Row RDD

    //    val conf = new SparkConf().setAppName("first").setMaster("local[*]").set("spark.driver.allowMultipleContexts","true")
    //    val sc = new SparkContext(conf)
    //    sc.setLogLevel("ERROR")

    //    val spark = SparkSession.builder().getOrCreate()
    //    import spark.implicits._
    //    val data = sc.textFile("/user/<LABUSER>/datatxns.txt")
    //    data.foreach(println)
    //    println

    //    val mapsplit = data.map(x => x.split(","))
    
    println
    println("ROW RDD")
    println

    val rowrdd = mapsplit.map(x => Row(x(0), x(1), x(2), x(3)))
    val prodfilter1 = rowrdd.filter(x => x(2).toString().contains("Gymnastics"))
    prodfilter1.foreach(println)

//    prodfilter1.foreach(println)
    val simpleSchema = StructType(Array(
      StructField("id", StringType),
      StructField("category", StringType),
      StructField("product", StringType),
      StructField("mode", StringType)))
      
    println
    println("ROW RDD full")
    println
    val fulldf = spark.createDataFrame(rowrdd, simpleSchema)
    fulldf.show()
      
    println
    println("ROW RDD after filter")
    println
    val dataframe1 = spark.createDataFrame(prodfilter1, simpleSchema)
    dataframe1.show()
    
    println
    println("DF after filter")
    println
    val filterdf = fulldf.filter(col("product") === "Pogo")
    filterdf.show()
    

  }
}