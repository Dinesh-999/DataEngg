
package apiReadCode

import scala.io.Source
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import scala.io._
import org.apache.spark.sql.functions._

import java.security.cert.X509Certificate
import javax.net.ssl._
import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.HttpClients
import org.apache.http.util.EntityUtils
import org.apache.spark.SparkContext


object apiReadCode {

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\hadoop")
    val conf = new SparkConf().setAppName("first").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("Error")
    
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    
    val sslContext = SSLContext.getInstance("TLS")
    sslContext.init(null, Array(new X509TrustManager {
      override def getAcceptedIssuers: Array[X509Certificate] = Array.empty[X509Certificate]
      override def checkClientTrusted(x509Certificates: Array[X509Certificate],s:String): Unit = {}
      override def checkServerTrusted(x509Certificates: Array[X509Certificate], s: String): Unit = {}
    }), new java.security.SecureRandom())
    
    val hostnameVerifier = new HostnameVerifier {
      override def verify(s:String, sslSession: SSLSession): Boolean = true
    }
    
    val httpClient = HttpClients.custom().setSSLContext(sslContext).setSSLHostnameVerifier(hostnameVerifier).build()
    val content = EntityUtils.toString(httpClient.execute(new HttpGet("https://randomuser.me/api/0.8/?results=10")).getEntity)
    val urlstring = content.mkString
    
    println(urlstring)
    val df = spark.read.json(sc.parallelize(List(urlstring)))
    df.show()
    df.printSchema()
    
    val flatdf = df.withColumn("results", explode(col("results"))).select("nationality", "seed",
        "version", "results.user.username", "results.user.cell", "results.user.dob", "results.user.email", "results.user.gender", 
        "results.user.location.city", "results.user.location.state", "results.user.location.street", "results.user.location.zip", "results.user.md5",
        "results.user.name.first", "results.user.name.last", "results.user.name.title",
        "results.user.password", "results.user.phone", "results.user.picture.large", "results.user.picture.medium",
        "results.user.picture.thumbnail", "results.user.registered", "results.user.salt", "results.user.sha1",
        "results.user.sha256")
        
      flatdf.show()
      
  }

}