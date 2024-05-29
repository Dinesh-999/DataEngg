package scenarios

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object listScenario {

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\hadoop")
    val conf = new SparkConf().setAppName("first").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("Error")
    
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    
    val lis1 = List(1, 2, 3)
    println(lis1)
    
    val lis2 = List("one", "two", "three")
    println(lis2)
    
    val lis1df = lis1.toDF("c1")
    lis1df.show()
    
    val lis2df = lis2.toDF("c2")
    lis2df.show()
    
    val rolist1 = lis1df.withColumn("id", monotonically_increasing_id())
    rolist1.show()
    
    val rolist2 = lis2df.withColumn("id", monotonically_increasing_id())
    rolist2.show()
    
    val joindf = rolist1.join(rolist2, Seq("id"), "inner")
    joindf.show()
    
    val concat = joindf.withColumn("final", expr("concat(c1,' is ',c2)"))
    concat.show()
    
    val finaldf = concat.select("final")
    finaldf.show()
  }

}