package spark60days

import org.apache.spark.sql.SparkSession

object day_7 {
  
  //Caching Spark DataFrame
  def main (args:Array[String]): Unit = {
    
    val spark = SparkSession.builder().master("local[*]").appName("day_7").getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
   
    val df1 = spark.read.csv("C:/Users/DELL/workspace/Spark/datasets/chapters-201108-004545.csv")
   // df1.show(false)
    df1.cache()

    for (column <- df1.columns) {
      //println("printing  columns " + column)
      val unique_values = df1.select(column).distinct().count()
      println("column " + column + " - " + unique_values)
      /*if (unique_values == 1) 
        println(column)
      */
    }
  }
}