package Basics

import org.apache.spark.SparkContext

object variable {

  def main(args: Array[String]): Unit = {

    println("----- variable ------")
    val a = 2
    println(a)

    val b = a + 2
    println(b)

    println("----- string ------")
    val c = "dinesh"
    println(c)

    val d = "hello" + c
    println(d)

    println("----- List ------")
    val lis = List(1, 2, 3, 4)
    println(lis)
    lis.foreach(println)

    var strlis = List("hello", "dinesh", "how ru")
    strlis.foreach(println)

    println("----- map ------")
    //   => lambda
    val res = lis.map(x => x + 2)
    println(res)

    println("----- filter ------")

    var fil = lis.filter(x => x > 2)
    println(fil)

  }
}