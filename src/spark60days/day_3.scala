package spark60days

import org.apache.spark.sql.SparkSession

object day_3 {
  
  //Read/Load CSV File into RDD and Data frame.
  def main(args:Array[String]):Unit = {
    
    val spark = SparkSession.builder.master("local[*]").appName("day_3").getOrCreate()
    val sc = spark.sparkContext
    
    //reading into rdd
    val rdd = sc.textFile("C:/Users/DELL/workspace/Spark/datasets/customers-orders.csv")
    rdd.take(10).foreach(println)
    rdd.take(10).foreach(f => {
      println(f)
    })
    
   println("*************************************************************")
    //reading data into DataFrame
    val DF = spark.read.option("header","false").
             option("inferSchema","true").
             csv("C:/Users/DELL/workspace/Spark/datasets/customers-orders.csv")
             
       DF.show(10, false)
       
  }
}