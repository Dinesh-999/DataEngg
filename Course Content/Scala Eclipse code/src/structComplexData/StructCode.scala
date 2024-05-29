package structComplexData
//  struct complex data - type 2 - json data
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object StructCode {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("first").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("Error")

    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    val df = spark.read.format("json")
      .option("multiline", "true")
      .load("file:///D:/Data Analytics applications/Eclipse Projects/data_files/jl.json")

    df.show()
    df.printSchema()

    val flattendf = df.select("id", "institute", "trainer",
      "location.permanentLocation", "location.temporaryLocation")
    flattendf.show()
    flattendf.printSchema()

    //    ===========================================================================

    val jsondf = spark.read.format("json")
      .option("multiline", "true")
      .load("file:///D:/Data Analytics applications/Eclipse Projects/data_files/jk.json")
    jsondf.show()
    jsondf.printSchema()

    val selectflattendf = jsondf.select("id", "institute", "location.*", "worklocation")
    selectflattendf.show()
    selectflattendf.printSchema()

    val complexdf = selectflattendf.select(col("id"), col("institute"),
      struct(
        col("permanentLocation"),
        col("temporaryLocation"),
        col("worklocation")).as("allLocations"))
    complexdf.show()
    complexdf.printSchema()

    val complexdf_withColumn = selectflattendf
      .withColumn(
        "allLocations", expr("""struct(permanentLocation, temporaryLocation, worklocation) """))
      .drop("permanentLocation", "temporaryLocation", "worklocation")
    complexdf_withColumn.show()
    complexdf_withColumn.printSchema()

    //    ===========================================================================

    val jkndf = spark.read.format("json")
      .option("multiline", "true").load("file:///D:/Data Analytics applications/Eclipse Projects/data_files/jkn.json")
    jkndf.show(false)
    jkndf.printSchema()

    val arrayexplode = jkndf.withColumn("Students", expr("explode(Students)"))
    arrayexplode.show()
    arrayexplode.printSchema()

    val finalflatten = arrayexplode.select("Students.user.*", "id", "institute")
    finalflatten.show()
    finalflatten.printSchema()

    //    ===========================================================================

    val jlndf = spark.read.format("json")
      .option("multiline", "true")
      .load("file:///D:/Data Analytics applications/Eclipse Projects/data_files/jln.json")
    jlndf.show(false)
    jlndf.printSchema()

    val jlnflattendf = jlndf.withColumn("Students", expr("explode(Students)"))
    jlnflattendf.show(false)
    jlnflattendf.printSchema()

    //    ========================================================================================

  }

}