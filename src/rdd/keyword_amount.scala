//No : 8

package rdd

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkContext

object keyword_amount extends App  {
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val sc = new SparkContext("local[*]","keyword_amount")
  
  val input = sc.textFile("file:///C:/Users/DELL/workspace/Spark/datasets/keyword_amount.csv")
  // Big data contents,24.06

  val mappedinput = input.map( x => (x.split(",")(1).toFloat,x.split(",")(0)))
  //24.06,Big data contents
  
  val words = mappedinput.flatMapValues( x => x.split(" "))
  // ((24.06,Big),(24.06,data),(24.06,contents))
  
  val finalmapped = words.map( x => (x._2.toLowerCase(),x._1))
  //((big,24.06),(data,24.06),(contents,24.06))
  
  val total = finalmapped.reduceByKey( (x,y) => (x+y))
  //(big,48.26)
  //(data,36.12)
  
  val sorted = total.sortBy(x => x._2,false)
  
  sorted.take(100).foreach(println)
  
}