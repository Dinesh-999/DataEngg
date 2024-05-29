package dataframe

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object snowflake {

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\hadoop")
    val conf = new SparkConf().setAppName("first").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("Error")
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    val today = "31-08-2023"

    val snowdf = spark
      .read
      .format("snowflake")
      .option("sfURL", "*************")
      .option("sfAccount", "******")
      .option("sfUser", "*********")
      .option("sfPassword", "*********")
      .option("sfDatabase", "zeyodb")
      .option("sfSchema", "zeyoschema")
      .option("sfRole", "ACCOUNTADMIN")
      .option("sfWarehouse", "COMPUTE_WH")
      .option("query", "select a.*,c.location from (select a.,b.prod from zeyotab a join zeyoprod b on a.id=b.id) a join zeyoloc c on a.id=c.id;")
      .load()
    snowdf.show(false)
    
    val snowdf_delete = snowdf.withColumn("current_date", lit(today))
      .withColumn("delete_ind", lit(0))
    snowdf_delete.show(false)
    snowdf_delete.printSchema()
    
    val schema = StructType(Array(
      StructField("name", StringType, nullable = false),
      StructField("Students", ArrayType(StringType), nullable = false)))
      
    val jsondf = snowdf_delete.withColumn(
      "JDATA",
      from_json(col("JDATA"), schema))
    jsondf.show(false)
    jsondf.printSchema()
    
    val exploded = jsondf.withColumn("name", expr("JDATA.name"))
      .withColumn("Students", expr("explode(JDATA.Students)")).drop("JDATA")
    exploded.show(false)
    exploded.printSchema()
    
    exploded.write.format("csv").partitionBy("current_date", "delete_ind")
      .mode("append").save("file:///C:/data/scnewdata")

  }

}