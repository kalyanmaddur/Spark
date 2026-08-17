// No : 12
//reduceByKey vs groupByKey

package rdd

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object log_level_grouping extends App {
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val sc = new SparkContext("local[*]","log_level_grouping")
  
  val rdd = sc.textFile("file:///C:/Users/DELL/workspace/Spark/datasets/bigLog.txt")
  
  val mappedRdd = rdd.map(x => {
    //(x.split(":")(0),x.split(",")(1))   -- splitting the data into 2 variables   
    val fields = x.split(":")
    (fields(0),fields(1))
  })
  
  /*
   * println("groupByKey -- "+ mappedRdd.groupByKey.collect())
   * println("reduceByKey -- "+ mappedRdd.reduceByKey.collect())
   * 
   *  Here collect() returns the Array[(String, Iterable[String])]  where string is the key and the iterable[string] is the values associated with the key
   */
  
  mappedRdd.groupByKey.collect().foreach(x => println(x._1,x._2.size))
  
  mappedRdd.reduceByKey(_+_).collect.foreach(x => println(x._1,x._2.size))
  
  scala.io.StdIn.readLine() //takes input from reader
}