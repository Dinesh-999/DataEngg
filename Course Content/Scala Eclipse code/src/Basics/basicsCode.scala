package Basics

object basicsCode {
  def main(args: Array[String]): Unit = {
    // variables
    println("Started")

    val a = 2
    println("====raw data===")
    println(a)

    val b = a + 1
    println("====proc data====")
    println(b)

    // strings
    val c = "zeyobron"
    println("===raw data====")
    println(c)

    val d = c + " ANALYTICS"
    println("==proc data==")
    println(d)

    // arithmetic operations
    val e = 2
    println("====raw data===")
    println(e)

    val f = a + 1
    println("====proc data====")
    println(f)

    //list
    val lis = List(1, 2, 3, 4)
    println("====list data====")
    println(lis)
    lis.foreach(println)

    // list - map
    println("====map list data====")
    val result = lis.map(x => x + 1)
    println(result)
    // list filter

    val lisin = List("zeyobron", "analytics", "zeyo")
    lisin.foreach(println)

    println("===proc list=====")
    println("====filter list data====")
    val fillis = lisin.filter(x => x.contains("zeyo"))
    fillis.foreach(println)
    //replace

    println("====replace data in list data====")
    lisin.foreach(println)
    val maplis = lisin.map(x => x.replace("zeyo", "tera"))
    maplis.foreach(println)

    // ============================================================================

    println("===started Zeyo====")
    val lisstr = List(
      "State->Telangana",
      "State->Gujarat",
      "State->Karnataka")

    lisstr.foreach(println)

    val mapstr = lisstr.map(x => x.replace("State->", ""))
    println
    println("=====replace list====")
    println
    mapstr.foreach(println)
    
    val lisstr1 = List("State~City")
    println
    println("=====before flatten===")
    lisstr1.foreach(println)

    val flatdata = lisstr1.flatMap(x => x.split("~"))
    println
    println("=====after flatten===")
    println
    flatdata.foreach(println)
    
    val mapdata = lisstr1.map(x => x.split("~"))
    println
    println("=====after map===")
    println
    mapdata.foreach(println)

    val lisstr2 = List(
      "State->Telangana",
      "City->Hyderabad",
      "State->Karnataka")

    println
    println("=====before filter===")
    println
    lisstr2.foreach(println)
    println
    println("=====after filter===")
    println
    val filterdata = lisstr2.filter(x => x.contains("State"))
    filterdata.foreach(println)

    // ============================================================================

    println("===started Zeyo====")
    println
    val lisstrstate = List(
      "state->Telangana~city->Hyderabad",
      "state->Karnataka~city->bangalore")
    println
    
    println("==raw List===")
    println
    lisstrstate.foreach(println)
    println
    
    println("==flat List===")
    println
    val stateflatdata = lisstrstate.flatMap(x => x.split("~"))
    stateflatdata.foreach(println)
    
    println
    println("=====state filter List======")
    println
    val stlist = stateflatdata.filter(x => x.contains("state"))
    stlist.foreach(println)
    
    println
    println("=====city filter List======")
    println
    val clist = stateflatdata.filter(x => x.contains("city"))
    clist.foreach(println)
    
    println
    println("=====state replace List======")
    println
    val statelist = stlist.map(x => x.replace("state->", ""))
    statelist.foreach(println)
    
    println
    println("=====city replace List======")
    println
    val citylist = clist.map(x => x.replace("city->", ""))
    citylist.foreach(println)

  }

}