// No: 7

package rdd

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import scala.math.min

object mintemp extends App {
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val sc = new SparkContext("local[*]","temperatures list")
  
  val inputdata = sc.textFile("file:///C:/Users/DELL/workspace/Spark/datasets/temperaturedata.csv")
  
  def tempData(record:String) = {
    
    val values = record.split(",")
    
    val ID = values(0)
    val readingtype = values(2)
    val temperature  = values(3).toInt 
    (ID,readingtype,temperature)
  }
  
  val mappeddata = inputdata.map(tempData)
  
  val filterdata = mappeddata.filter(x => x._2 == "TMIN")
  
  val temp = filterdata.map(x => (x._1,x._3))
  
  val finaldata = temp.reduceByKey((x,y) => min(x,y)).sortBy( x => x._2) // asceding by default, descding sortBy( x => x._2,false)
  println("==========minimum temperature data==================")
  
  finaldata.collect.foreach(println)
  
}
  
  /* 
   * val inputdata = sc.textFile("file:///C:/Users/DELL/workspace/Spark/datasets/temperaturedata.csv")
   * 
   * def parseLine(line:String) = {
   * val fields = line.split(",")
   * val id =  fields(0)
   * val type = fields(1)
   * val tempe = fields(3)
   * (id,type,tempe)
   * }
   * 
   * val parsedLines = inputdata.map(parseLine)
   * val minTemps = parsedLines.filter(x => x._2 == "TMIN")
   * val id_temps = minTemps.map(x => (x._1, x._3.toFloat))
   * 
   * val minTempbyID = id_temps.reduceByKey( (x,y) => min(x,y))
   * 
   * val results = minTempbyID.collect()
   * 
   * for(results <- results.sorted) {
   * val station = result._1
   * val temp = result._2
   * val formattedTemp = f"$temp%.2f F"
   * println(s"station minimum temperature: $formattedTemp")
   */