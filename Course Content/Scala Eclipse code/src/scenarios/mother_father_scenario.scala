package scenarios

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object mother_father_scenario {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C://hadoop")

    val conf = new SparkConf().setAppName("first").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    val r = spark.read.format("csv").option("header", "true").load("file:///C:/Users/dines/Downloads/data analytics/data/r.csv")
    r.show()

    val p = spark.read.format("csv").option("header", "true").load("file:///C:/Users/dines/Downloads/data analytics/data/p.csv")
    p.show()

    val leftjoin = r.join(p, r("cid") === p("id"), "left").select("cid", "pid", "name")
    leftjoin.show()

    val antijoin = p.join(r, r("cid") === p("id"), "left_anti")
    antijoin.show()

    val mothers = antijoin.filter(col("gender") === "M").withColumnRenamed("name", "mothers")
    mothers.show()

    val fathers = antijoin.filter(col("gender") === "F").withColumnRenamed("name", "fathers")
    fathers.show()

    val mothersjoin = leftjoin.join(mothers, leftjoin("pid") === mothers("id"), "left").select("cid", "pid", "name", "mothers")
    //                              .join(fathers, leftjoin("pid") === fathers("id"), "left")
    mothersjoin.show()

    val fathersjoin = mothersjoin.join(fathers, mothersjoin("pid") === fathers("id"), "left")
    fathersjoin.show()

    val finaldf = fathersjoin.select("name", "mothers", "fathers")
    finaldf.show()

  }
}