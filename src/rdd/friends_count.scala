// No : 5

package rdd

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkContext

object friends_count extends App{
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val sc = new SparkContext("local[*]","friends")
  
  val input = sc.textFile("file:///C:/Users/DELL/workspace/Spark/datasets/friends-data.csv")
  
  def parseline(line:String) = {
    
    val fields = line.split(",")
    val age = fields(2).toInt
    val friedsNo = fields(3).toInt
    (age,friedsNo)
  }
  val mappedInput = input.map(parseline) // writing a named function
  /*output
   * (33,100)
   *(33,100)
   * (33,200)
   */
  val mappedFinal = mappedInput.map(x => (x._1,(x._2,1))) // mapValues( x => (x,1)) -- deals only with values
  /*output
   * (33,(100,1))
   *(33,(100,1))
   * (33,(200,1))
   */
  val totalByAge = mappedFinal.reduceByKey((x,y) => (x._1 + y._1, x._2 + y._2))
  /*output
   * (33,(600,3))
   * (47,(500,2))
   */
  val avgByAge = totalByAge.map( x => (x._1,(x._2._1/x._2._2))).sortBy(x => x._1)
  
  // totalByAge.mapValues( x => x._1/x._2)  -- mapValues deals only with values and the key is not altered
  // x._1 --> 600
  // x._2 --> 3
  
  
  avgByAge.collect.foreach(println)
  /*output
   * (33,200)
   * (47,250)
   */
  
  
  /* // another approach wit same result
  val split = input.map(x => x.split(",")) // anonymous function
  
  val mappedData = split.map( x => (x(0).toInt,x(2).toInt,(x(3).toInt,1)))
  
  /*output is array of tuples -- [(a,b,(c,d))]
   *we can access tuples wit x._1 -- tuple is 1 based index
   * and accessing the tuple inside a tuple is (a,b,(c,d))
   *
   * x._1 --> a ,, x._3._1 --> c
   */
  
  val mapdata2 = mappedData.map( x => (x._2,x._3))
  
  /*reduceByKey takes two ele 
   * 1.(a,(c,d)) , 2.(e,(g,h))
   *  x,y => x+y   
   *  x,y are two elements , x+y are their respective values
   *  here values are (c,d) -- x and (g,h) -- y
   */
  
  val reducedData = mapdata2.reduceByKey((x,y) => (x._1 + y._1,x._2 + y._2))
  
  println ("age : frnds_count")
  val avg = reducedData.map( x => (x._1,(x._2._1/x._2._2)))
 
  avg.collect.foreach(println)
  */
}