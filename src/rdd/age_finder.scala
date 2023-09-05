// No: 6

package rdd

import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger

object age_finder extends App  {
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val sc = new SparkContext("local[*]", "age_check")
  
  val inputdata = sc.textFile("file:///C:/Users/DELL/workspace/Spark/datasets/peoples_list.csv")
  
  inputdata.collect.foreach(println)
  
  def ageCheck(data:String) = {
    
    val record = data.split(",")
    
    if (record(1).toInt > 18 ) (record(0),record(1).toInt,record(2), "Y") else (record(0),record(1).toInt,record(2),"N")
  
  }
  
  val finaldata = inputdata.map(ageCheck).sortBy(x => x._2)
  
  println("============================Final Result============================")
  finaldata.collect.foreach(println)
  
}
  
/*
 *val rd1 = spark.sparkContext.textFile("file:///C:/Users/DELL/workspace/Spark/datasets/peoples_list.csv")
 *rd1.collect().foreach(println)
 *
 * val rd2 = rd1.map(line => 
 * {
 * 	val fields = line.split(",")
 * 	if(fields(1).toInt > 18)
 *    (fields(0),fields(1),fields(2),"Y")
 *  else
 *    (fields(0),fields(1),fields(2),"N") 
 *	})
 * rdd2.collect().foreach(println)
 * 
 */