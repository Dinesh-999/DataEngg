package Basics

object distinct {
   def main(args: Array[String]): Unit = {
     
     var duplicatelist = List("how","are","you","how", "you", "what")
     println(duplicatelist)
     
     println("----- distinct -----")
     var uniq = duplicatelist.distinct
     println(uniq)
     
     var dupnum = List( 1, 2,3, 4,5, 3,3,32,2,1)
     var uniqnum = dupnum.distinct
     println(uniqnum)
     
     
   }
}