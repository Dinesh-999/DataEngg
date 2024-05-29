package Basics

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object readRDD {
  def main(args: Array[String]): Unit = {

    //    System.setProperty("hadoop.home.dir", "C:\\hadoop")

    val conf = new SparkConf().setAppName("first").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val data = sc.textFile("data/data1.txt")
//        val data = sc.textFile("file:///C:/Users/dines/Downloads/data analytics/scala eclipse working/spark first/data/data1.txt")
    println("---Raw data---")
    println()
    data.foreach(println)
    println()

    val splited = data.flatMap(x => x.split("~"))
    println("--- flatmap data ---")
    println()
    splited.foreach(println)
    println()

    val state = splited.filter(x => x.contains("state->"))
    println("--- state filter data ---")
    println()
    state.foreach(println)
    println()
    
    val city = splited.filter(x => x.contains("city->"))
    println("--- city filter data ---")
    println()
    city.foreach(println)
    println()
    
    val statename = state.map(x => x.replace("state->", ""))
    println("--- state replace data ---")
    println()
    statename.foreach(println)
    println()
    
    val cityname = city.map(x => x.replace("city->", ""))
    println("--- city replace data ---")
    println()
    cityname.foreach(println)
    println()
    
    
//    statename.coalesce(1).saveAsTextFile("C:/Data engg/statedata")
//    cityname.coalesce(1).saveAsTextFile("C:/Data engg/citydata")
//    getting error while saving to file
    

  }
}