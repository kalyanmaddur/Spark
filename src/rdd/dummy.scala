// No: 0

package rdd

import org.apache.log4j.Level
import org.apache.spark.SparkContext
import org.apache.log4j.Logger

object dummy extends App {
  
    Logger.getLogger("org").setLevel(Level.ERROR)
  
  val sc = new SparkContext("local[*]","top_10")
  
  val input = sc.textFile("file:///C:/Users/DELL/workspace/Spark/datasets/customers-orders.txt")
  
  input.flatMap(x => x.split("\t")).collect.foreach(println) // spliting with tab delimiter
  
}