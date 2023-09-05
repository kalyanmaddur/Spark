// No: 4

package rdd

import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.SparkContext

object ratings_calculator extends App{
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val sc = new SparkContext("local[*]","ratings")
  
  val input = sc.textFile("file:///C:/Users/DELL/workspace/Spark/datasets/movie-data.txt")
  
  val split = input.map(x => x.split("\t")(2))
  
  val ratings = split.map((_,1)) // .map(x => (x,1))
  val ratings_count = ratings.reduceByKey( (x,y) => x+y) // .reduceByKey(_+_)
  
  // insted of mapping and reduceByKey we can use CountByValue
  // 1.map(trnsf) + reduceByKey(trnsf) ---> rdd -- gives parallelism
  // 2.countByValue (action) ---> local variable
  // if we need to perform other other operations after the counting, we need to use (1), if no operations use (2)
  
  println("ratings --- no of movies")
  ratings_count.collect.foreach(println)
  
  println("result of countbyvalue")
  split.countByValue.foreach(println)
}