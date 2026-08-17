// No: 3

package rdd

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object total_spent extends App{
  
   Logger.getLogger("org").setLevel(Level.ERROR)
  
  val sc = new SparkContext("local[*]","total_spent")
  
  val input = sc.textFile("file:///C:/Users/DELL/workspace/Spark/datasets/customers-orders.csv")
  
  val split_data = input.map(x => (x.split(",")(0).toInt,x.split(",")(2).toFloat))
  
  //split once and reuse 
  /*
   * val split_data  = input .map(x => 
    {	
    val ele = x.split(",")
    (ele(0).toInt,ele(2).toFloat) 
    })
   * 
   */
  
  val total = split_data.reduceByKey((x,y) => (x+y))
  
  val result = total.sortBy(x => x._2) // sortBy(_._2)
  
  result.collect.foreach(println)
  

}