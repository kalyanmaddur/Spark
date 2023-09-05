// No : 9

package rdd

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.metrics.source.Source
import scala.io.Source



object words_filter extends App {
  
  
   def filteredWords():Set[String] = {
   
   var words:Set[String] = Set() // initializing an empty set
   val lines = Source.fromFile("/Users/DELL/workspace/Spark/datasets/boring_words.txt").getLines()
   // to open a file locally and get the lines 
   // lines is a local variable not a rdd
   
   for( line <- lines) {
     words = words + line // words += line
   }
   println(words)
   words // returning words as set 

    }
  
  
 Logger.getLogger("org").setLevel(Level.ERROR)
  
  val sc = new SparkContext("local[*]","keyword_amount")
 
  var nameSet = sc.broadcast(filteredWords) // broadcasting the data 

  val input = sc.textFile("file:///C:/Users/DELL/workspace/Spark/datasets/keyword_amount.csv")
  // Big data contents,24.06

  val mappedinput = input.map( x => (x.split(",")(1).toFloat,x.split(",")(0)))
  //24.06,Big data contents
  
  val words = mappedinput.flatMapValues( x => x.split(" "))
  // ((24.06,Big),(24.06,data),(24.06,contents))
  
  val finalmapped = words.map( x => (x._2.toLowerCase(),x._1))
  //((big,24.06),(data,24.06),(contents,24.06))
  
  
  val fiteredwords =finalmapped.filter(x => !nameSet.value(x._1)) // filter returns that are present, here we want to filter the words and get which are not present in the boring words list
                                                // hence we are using negation       
  // .value() checks whether the value entered is present or not, returns boolean  
  
  val total = fiteredwords.reduceByKey( (x,y) => (x+y))
  //(big,48.26)
  //(data,36.12)
  
  val sorted = total.sortBy(x => x._2,false)
  
  sorted.take(100).foreach(println)
 
  
}