package scenarios

object listScenario1 {
  def main(args: Array[String]): Unit = {
   val mylist = List(10,5,24,"Hi",90,12,"Hello")
    mylist.collect{
    case num : Int => println(num)
}
  }
}