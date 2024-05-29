package dataframe

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import spray.json._

object SnowFlakePart2 {
  
  
//  data for this code
  
//  ID,DATETIME,ydata,PROD,LOCATION
//  1,2023-06-30,{"name":"zeyo","Students":["rajesh", "vishnu"]},Telecom,Livingston
//  2,2023-06-30,{"name":"rani","Students":["vijay", "hema"]},Healthcare,Anchorage
//  3,2023-06-30,{"name":"vasi","Students":["harika", "gowtham"]},Ecommerce,Butler
//  4,2023-06-30,{"name":"visu","Students":["shaan", "ravi"]},Telecom,Ashland

//
//  ID,DATETIME,tdata,PROD,LOCATION
//  2,2023-07-01,{"name":"ran","Students":["vijay", "hema"]},Healthcare,Anchorage
//  4,2023-07-01,{"name":"visu","Students":["sonu","ravi"]},Telecom,Ashland
//  5,2023-07-01,{"name":"siji","Students":["vishali", "sheen"]},Healthcare,Cook
//
//  tdata, ydata is string
  

  def jcom = udf((tj: String, yj: String) => {
    
    val json2 = JsonParser(yj)
    val json1 = JsonParser(tj)
    val results = json1 == json2
    results
    
  })

  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "C:\\hadoop")
    val conf = new SparkConf().setAppName("first").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("Error")
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    val today = "31-08-2023"
    val yesterday = "01-09-2023"

    val snowdf = spark
      .read
      .format("snowflake")
      .option("sfURL", "https://vvapryv-sg46500.snowflakecomputing.com")
      .option("sfAccount", "********")
      .option("sfUser", "********")
      .option("sfPassword", "********")
      .option("sfDatabase", "********")
      .option("sfSchema", "********")
      .option("sfRole", "ACCOUNTADMIN")
      .option("sfWarehouse", "COMPUTE_WH")
      .option("query", s"""select a.*, c.location from (
                            select a.* ,b.prod from zeyotab a join zeyoprod b on a.id=b.id) a 
                            join zeyoloc c on a.id=c.id
                            where current_date in ('$yesterday' , '$today')
                            ;""")
      .load()
    snowdf.show(false)

    val ydata = snowdf.filter(col("DATETIME") === yesterday).withColumnRenamed("JDATA", "ydata")
    ydata.show(false)

    val tdata = snowdf.filter(col("DATETIME") === today).withColumnRenamed("JDATA", "tdata")
    tdata.show(false)

    val deleted = ydata.join(tdata, Seq("ID"), "left_anti")
    deleted.show()

    val newly = tdata.join(ydata, Seq("ID"), "left_anti")
    newly.show()

    val innerjoin = tdata.join(ydata.select("ID", "ydata"), Seq("ID"), "inner")
    innerjoin.show(false)

    val udfresult = innerjoin.withColumn("compare", jcom(col("tdata"), col("ydata")))
    udfresult.show(false)

    val finalchanged = udfresult.filter(col("compare") === false).drop("ydata", "compare")
      .withColumn("current_date", lit(today)).withColumn("delete_ind", lit(2))
    finalchanged.show()

    val finaldeleted = deleted.withColumn("current_date", lit(today)).withColumn("delete_ind", lit(1))

    val finalnewly = newly.withColumn("current_date", lit(today)).withColumn("delete_ind", lit(0))

    val finaltodaydf = finaldeleted.union(finalnewly).union(finalchanged).withColumnRenamed("ydata", "JDATA")
    finaltodaydf.show()

    val schema = StructType(Array(
      StructField("name", StringType, nullable = false),
      StructField("Students", ArrayType(StringType), nullable = false)))

    val jsondf = finaltodaydf.withColumn("JDATA", from_json(col("JDATA"), schema))
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