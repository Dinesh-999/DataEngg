package test

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import spray.json._

object test1 {

  def main(args: Array[String]): Unit = {

    //    val conf = new SparkConf().setAppName("first").setMaster("local[*]")
    //    val sc = new SparkContext(conf)
    //    sc.setLogLevel("ERROR")
    //
    //    val split_dipt = sc.textFile("file:///D:/Data Analytics applications/Eclipse Projects/scenarios_data/split_dipt.txt")
    //    split_dipt.foreach(println)
    //
    //    val splitted_dipt = split_dipt.map(x => x.split(","))
    //    splitted_dipt.foreach(println)
    //
    //    val data = splitted_dipt.map( x => List(x(0), x(1)))
    //    data.foreach(println)

    // ===========================================================
    //  read only numbers/int from list
    //    val mylist = List(10, 5, 24, "Hi", 90, 12, "Hello")
    //    mylist.collect {
    //      case num: Int => println(num)
    //    }

    // ===========================================================

//    compare 2 json strings
    val json1 = JsonParser("""{"name":"rani", "Students": ["vijay","hema"]}""")
    val json2 = JsonParser("""{"name":"rani", "Students":["vy, "hema"]}""")
    val result = json1 == json2

    println(result)
    
    // ===========================================================

  }
}
