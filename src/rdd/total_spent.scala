// No: 3

package rdd

import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger

object total_spent extends App{
  
   Logger.getLogger("org").setLevel(Level.ERROR)
  
  val sc = new SparkContext("local[*]","total_spent")
  
  val input = sc.textFile("file:///C:/Users/DELL/workspace/Spark/datasets/customers-orders.csv")
  
  val split_data = input.map(x => (x.split(",")(0).toInt,x.split(",")(2).toFloat))
  //val split_data  = input .map(x => (x.split(",")(0)(2)))
  
  val total = split_data.reduceByKey( (x,y) => (x+y))
  
  val result = total.sortBy(x => x._2) // sortBy(_._2)
  
  total.collect.foreach(println)
  

}