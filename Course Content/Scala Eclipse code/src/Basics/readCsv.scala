package Basics

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object readCsv {
  def main(args: Array[String]): Unit = {

    println("hello")
    System.setProperty("hadoop.home.dir", "C:/hadoop")

    val conf = new SparkConf().setAppName("first").setMaster("local[*]")
    val sc = new SparkContext(conf)

    sc.setLogLevel("ERROR")

    val data = sc.textFile("file:///C:/Users/dines/Downloads/data analytics/data/usdata.csv")
    //    data.foreach(println)
    data.take(10).foreach(println)

    println()
    println("===filter data ===")
    println()

    val lendata = data.filter(x => x.length() > 200)

    lendata.foreach(println)

    println()
    println("===flaten data ===")
    println()

    val flatdata = lendata.flatMap(x => x.split(","))
    flatdata.foreach(println)

    println()
    println("===replace data ===")
    println()

    val repdata = flatdata.map(x => x.replace("-", ""))
    repdata.foreach(println)

    //    concact with repdata with zeyo
    println()
    println("===concat data ===")
    println()

    val condata = repdata.map(x => x + ",zeyo")
    condata.foreach(println)

    condata.coalesce(1).saveAsTextFile("file:///C:/Users/dines/Downloads/data analytics/write_data/uspro")
    println()
    println("===data written ===")
    println()
  }
}