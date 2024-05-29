package Basics

object SecondDay {
  def main(args: Array[String]): Unit = {

    var nameslist = List("zeyobron", "analytics", "zeyo")
    println(nameslist)

    println("----- filter and contains ------")
    var filternames = nameslist.filter(x => x.contains("zeyo"))
    println(filternames)

    println("----- map and replace ------")
    var replacenames = nameslist.map(x => x.replace("zeyo", "tera"))
    println(replacenames)

    println("----- flatMap ------")
    var strlist = List("A~B", "C~D", "E~F")
    var splitlist = strlist.flatMap(x => x.split("~"))
    println(splitlist)
    //    dont use map with split function, use flatmap while using split

    var splitlist1 = strlist.map(x => x.split("~"))
    println(splitlist1)

    println("----- replace ------")

    var states = List("state->karnataka", "state->andra", "state->tamilnadu")

    var repla = states.map(x => x.replace("state->", ""))
    println(repla)

  }

}