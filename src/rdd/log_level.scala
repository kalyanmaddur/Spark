// No : 11
// counting the error and warn messages

// creating a rdd using a existing variable

package rdd

import org.apache.log4j.Logger


import org.apache.log4j.Level
import org.apache.spark.SparkContext

object log_level extends App {
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val sc = new SparkContext("local[*]","count")
  
  val list = List("WARN: Tuesday 4 September 0405",
      "ERROR: Tuesday 4 September 0405",
      "ERROR: Tuesday 4 September 0405",
      "ERROR: Tuesday 4 September 0405",
      "WARN: Tuesday 4 September 0405",
      "WARN: Tuesday 4 September 0405")
      
  val rdd = sc.parallelize(list) // creating a rdd by parallelizing an existing collection 
  
  val newrdd = rdd.map( x => {
    val columns = x.split(":")
    val message = columns(0)
    (message,1) // pair rdd
  })
  
  val finalrdd = newrdd.reduceByKey((x,y) => x + y)
  
  finalrdd.collect().foreach(println)
  
  /*
   * sc.parallelize(list).map(x => (x.split(":")(0),1)).
   * reduceByKey(_+_).collect().foreach(println)
   */
  
}