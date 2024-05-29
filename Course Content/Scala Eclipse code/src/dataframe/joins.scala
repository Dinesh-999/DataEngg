package dataframe

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object joins {

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\hadoop")

    println("====started==")
    val conf = new SparkConf().setAppName("first").setMaster("local[*]")
      .set("spark.driver.allowMultipleContexts", "true")

    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    val cust = spark.read.format("csv").option("header", "true").load("file:///C:/data/cust.csv")
    cust.show()

    val prod = spark.read.format("csv").option("header", "true").load("file:///C:/data/prod.csv")
    prod.show()

    println
    println("""========inner join============""")
    println
    val innerjoin = cust.join(prod, Seq("id"), "inner")
    innerjoin.show()

    println
    println("""========left join============""")
    println
    val left = cust.join(prod, Seq("id"), "left")
    left.show()

    println
    println("""========right join============""")
    println
    val right = cust.join(prod, Seq("id"), "right")
    right.show()

    println
    println("""========full join============""")
    println
    val full = cust.join(prod, Seq("id"), "full").orderBy("id")
    full.show()

    // ============================================================================
    //  scenario code
    
    val source = spark.read.format("csv").option("header", "true").load("file:///C:/data/source.csv")
    source.show()

    val target = spark.read.format("csv").option("header", "true").load("file:///C:/data/target.csv").withColumnRenamed("name", "name1")
    target.show()

    val fulljoin = source.join(target, Seq("id"), "full").orderBy("id")
    fulljoin.show()

    val match_mis = fulljoin.withColumn("comment", expr("case when name=name1 then 'Match' else 'Mismatch' end"))
    match_mis.show()

    val remvmatch = match_mis.filter(!(col("comment") === "Match"))
    remvmatch.show()

    val finaldfpre = remvmatch.withColumn("comment", expr("""case when name1 is null then 'New in Source' 
                  when name is null then 'New in Target' 
                  else comment end"""))
    finaldfpre.show()

    val finaldf = finaldfpre.drop("name", "name1")
    finaldf.show()

    //    ==========================================================

    val df11 = spark.read.format("csv")
      .option("header", "true")
      .load("file:///C:/data/d1.csv")
    df11.show()
    
    val df12 = spark.read.format("csv")
      .option("header", "true")
      .load("file:///C:/data/d2.csv")
    df12.show()
    
    val df13 = spark.read.format("csv")
      .option("header", "true")
      .load("file:///C:/data/d3.csv")
    df13.show()
    
    val alljoindf = df11.join(df12, Seq("id"), "left").join(df13, Seq("id"), "left")
    alljoindf.show()
    
    val joinwith = alljoindf
      .withColumn("salary", expr("case when salary is null then 0 else salary end"))
      .withColumn("salary1", expr("case when salary1 is null then 0 else salary1 end"))
      .withColumn("ns", expr("salary+salary1"))
    joinwith.show()
    
    val allfinaldf = joinwith.drop("salary", "salary1").withColumnRenamed("ns", "salary")
    allfinaldf.show()
    
 //   ===========================================
      
      
      

  }
}