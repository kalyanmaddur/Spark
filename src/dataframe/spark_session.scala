package dataframe

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf

object spark_session extends App {
  

  //creating spark session using builder method.
  val spark = SparkSession.builder()
                          .appName("Spark Application")
                          .master("local[*]")
                          .getOrCreate()
  
  //creating a spark configuration object which holds properties for spark session in key values.                       
  val sparkConf = new SparkConf()
  
  //setting key values for spark configuration object
  sparkConf.set("spark.app.name","Spark Application")
  sparkConf.set("spark.master","local[*]")
  
  //creating spark session with builder method and spark configuration object
  /*
  val spark = SparkSession.builder()
                          .config(sparkConf)
                          .getOrCreate()
  */
  
  //stop spark session
  spark.stop()
  
  
}